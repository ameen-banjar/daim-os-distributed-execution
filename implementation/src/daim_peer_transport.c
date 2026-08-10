#define _POSIX_C_SOURCE 200809L

#include "daim_peer_transport.h"
#include "daim_peer_protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MAX_CONNECTIONS 32
#define MAX_CONFIGURED_PEERS 31
#define MAX_THREADS (MAX_CONNECTIONS + MAX_CONFIGURED_PEERS + 1)
#define BACKOFF_BASE_MS 100
#define BACKOFF_CAP_MS 5000
#define STOP_POLL_MS 50
#define UNRESPONSIVE_PEER_TIMEOUT_MS 3000
#define LISTEN_BACKLOG 64

struct connection {
    int fd;
    int active;
    uint64_t peer_node_id;
    pthread_mutex_t send_lock;
};

struct configured_peer {
    char host[64];
    int port;
};

struct daim_peer_transport {
    pthread_mutex_t lock;
    pthread_cond_t accept_handlers_done;
    uint64_t node_id;
    uint64_t owner_epoch;
    int listen_fd;
    int stop; /* always read/written under lock -- see should_stop()/request_stop() */
    struct connection conns[MAX_CONNECTIONS];
    struct configured_peer peers[MAX_CONFIGURED_PEERS];
    size_t peer_count;
    pthread_t threads[MAX_THREADS];
    size_t thread_count;
    size_t active_accept_handlers;
    struct daim_peer_transport_callbacks callbacks;
    struct daim_peer_transport_stats stats;
    uint64_t next_message_id;
};

struct dial_thread_arg {
    struct daim_peer_transport *t;
    size_t peer_index;
};

struct accept_thread_arg {
    struct daim_peer_transport *t;
    int fd;
};

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void sleep_ms(int ms)
{
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

static void fire_lifecycle(struct daim_peer_transport *t, const char *event, uint64_t peer_node_id, uint64_t detail)
{
    if (t->callbacks.on_lifecycle) {
        t->callbacks.on_lifecycle(t->callbacks.context, event, peer_node_id, detail);
    }
}

static int should_stop(struct daim_peer_transport *t)
{
    int v;
    pthread_mutex_lock(&t->lock);
    v = t->stop;
    pthread_mutex_unlock(&t->lock);
    return v;
}

static void request_stop(struct daim_peer_transport *t)
{
    pthread_mutex_lock(&t->lock);
    t->stop = 1;
    pthread_mutex_unlock(&t->lock);
}

static void register_thread(struct daim_peer_transport *t, pthread_t th)
{
    pthread_mutex_lock(&t->lock);
    if (t->thread_count < MAX_THREADS) {
        t->threads[t->thread_count++] = th;
    }
    pthread_mutex_unlock(&t->lock);
}

/* --- raw send/recv of one framed message --- */

/* TCP send() may write fewer bytes than requested even on a blocking
   socket (short/partial writes are permitted by POSIX, not just a
   non-blocking-socket phenomenon); loop until the whole buffer is sent or
   a real error/EOF occurs. Returns len on success, -1 on failure. */
static ssize_t send_all(int fd, const uint8_t *buf, size_t len)
{
    size_t sent_total = 0;
    while (sent_total < len) {
        ssize_t n = send(fd, buf + sent_total, len - sent_total, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            errno = EPIPE;
            return -1;
        }
        sent_total += (size_t)n;
    }
    return (ssize_t)sent_total;
}

static int send_framed(struct daim_peer_transport *t, struct connection *conn, uint8_t message_type,
                        const uint8_t *payload, uint32_t payload_len)
{
    uint8_t buf[DAIM_PEER_HEADER_SIZE + DAIM_PEER_MAX_PAYLOAD_SIZE];
    struct daim_peer_header hdr;
    ssize_t sent;
    uint64_t msg_id;

    /* Every current caller passes a small, fixed-size payload (the HELLO/
       ACK/snapshot-begin payloads, or DAIM_PEER_HOST_LOCATION_WIRE_SIZE),
       so this can never actually trigger today -- but buf's size is
       derived from DAIM_PEER_MAX_PAYLOAD_SIZE, and memcpy below does not
       itself bounds-check, so this guard is what actually keeps a future
       caller (or a bug) from writing past buf's end instead of merely
       hoping no caller ever does. */
    if (payload_len > DAIM_PEER_MAX_PAYLOAD_SIZE) {
        return -1;
    }

    /* The connection slot is stable, but its fd can be closed and reused by
       a reconnecting thread.  Serialize the active check, frame creation,
       and write with teardown so a disseminator can never write through a
       stale fd or a destroyed mutex. */
    pthread_mutex_lock(&conn->send_lock);
    pthread_mutex_lock(&t->lock);
    if (!conn->active) {
        pthread_mutex_unlock(&t->lock);
        pthread_mutex_unlock(&conn->send_lock);
        errno = ENOTCONN;
        return -1;
    }
    msg_id = ++t->next_message_id;
    pthread_mutex_unlock(&t->lock);

    memset(&hdr, 0, sizeof(hdr));
    hdr.protocol_version = DAIM_PEER_PROTOCOL_VERSION;
    hdr.message_type = message_type;
    hdr.origin_node_id = t->node_id;
    hdr.message_id = msg_id;
    hdr.payload_length = payload_len;
    hdr.send_time_ns = monotonic_ns();
    daim_peer_encode_header(buf, &hdr);
    if (payload_len > 0) {
        memcpy(buf + DAIM_PEER_HEADER_SIZE, payload, payload_len);
    }

    sent = send_all(conn->fd, buf, DAIM_PEER_HEADER_SIZE + payload_len);
    pthread_mutex_unlock(&conn->send_lock);

    if (sent != (ssize_t)(DAIM_PEER_HEADER_SIZE + payload_len)) {
        return -1;
    }
    pthread_mutex_lock(&t->lock);
    t->stats.messages_sent++;
    t->stats.bytes_sent += (uint64_t)sent;
    pthread_mutex_unlock(&t->lock);
    return 0;
}

/* Blocking read of exactly one framed message. Returns 0 on success
   (header/payload filled in), -1 on any I/O error, short read, or a
   malformed/oversized header (daim_peer_decode_header's own checks). */
static int recv_framed(int fd, struct daim_peer_header *hdr, uint8_t *payload_buf, size_t payload_buf_cap)
{
    uint8_t header_buf[DAIM_PEER_HEADER_SIZE];
    ssize_t n = recv(fd, header_buf, DAIM_PEER_HEADER_SIZE, MSG_WAITALL);
    if (n != DAIM_PEER_HEADER_SIZE) {
        if (n == 0) {
            errno = ECONNRESET;
        } else if (n >= 0) {
            errno = EPROTO;
        }
        return -1;
    }
    if (daim_peer_decode_header(header_buf, DAIM_PEER_HEADER_SIZE, hdr) != 0) {
        errno = EPROTO;
        return -1;
    }
    if (hdr->payload_length > payload_buf_cap) {
        errno = EMSGSIZE;
        return -1;
    }
    if (hdr->payload_length > 0) {
        n = recv(fd, payload_buf, hdr->payload_length, MSG_WAITALL);
        if (n != (ssize_t)hdr->payload_length) {
            if (n == 0) {
                errno = ECONNRESET;
            } else if (n >= 0) {
                errno = EPROTO;
            }
            return -1;
        }
    }
    return 0;
}

/* --- snapshot exchange --- */

struct snapshot_send_ctx {
    struct daim_peer_transport *t;
    struct connection *conn;
    int failed;
};

static void snapshot_emit(void *context, const struct daim_host_location *entry)
{
    struct snapshot_send_ctx *ctx = context;
    uint8_t buf[DAIM_PEER_HOST_LOCATION_WIRE_SIZE];
    if (ctx->failed) {
        return;
    }
    daim_peer_encode_host_location(buf, entry);
    if (send_framed(ctx->t, ctx->conn, DAIM_PEER_MSG_STATE_SNAPSHOT_ENTRY, buf, sizeof(buf)) != 0) {
        ctx->failed = 1;
        return;
    }
    pthread_mutex_lock(&ctx->t->lock);
    ctx->t->stats.snapshot_entries_sent++;
    pthread_mutex_unlock(&ctx->t->lock);
}

static int serve_snapshot_request(struct daim_peer_transport *t, struct connection *conn)
{
    struct snapshot_send_ctx ctx = {t, conn, 0};
    uint8_t count_buf[4];
    int count;
    daim_peer_encode_snapshot_begin(count_buf, 0); /* count filled in after export */
    count = daim_host_export_snapshot(snapshot_emit, &ctx);
    if (count < 0 || ctx.failed) {
        return -1;
    }
    return send_framed(t, conn, DAIM_PEER_MSG_STATE_SNAPSHOT_END, NULL, 0);
}

/* Symmetric snapshot exchange, run by BOTH sides of every connection
   regardless of who dialed: each side sends its own STATE_SNAPSHOT_REQUEST
   and must receive a full response (ENTRY* + END) before moving to steady
   state, while also serving the peer's request whenever it arrives
   (before, after, or interleaved with its own response -- the two request/
   response flows are independent on a full-duplex socket). This matters
   because of the topology's dial rule (start_cluster in the gate driver:
   node i dials node j only for i<j, so every pair has exactly one
   connection instead of two): the highest-numbered node never dials
   anyone, so if only the dialer pulled a snapshot (the original design),
   that node could never resynchronise its own knowledge after a
   reconnect -- it would only ever see live updates from that point
   forward. Returns the number of entries this side applied from the
   peer's snapshot, or -1 on I/O failure. */
static int exchange_snapshots(struct daim_peer_transport *t, struct connection *conn)
{
    uint8_t payload[DAIM_PEER_MAX_PAYLOAD_SIZE];
    struct daim_peer_header hdr;
    int applied = 0;
    int my_snapshot_done = 0;

    if (send_framed(t, conn, DAIM_PEER_MSG_STATE_SNAPSHOT_REQUEST, NULL, 0) != 0) {
        return -1;
    }
    while (!my_snapshot_done) {
        if (recv_framed(conn->fd, &hdr, payload, sizeof(payload)) != 0) {
            return -1;
        }
        pthread_mutex_lock(&t->lock);
        t->stats.messages_received++;
        t->stats.bytes_received += DAIM_PEER_HEADER_SIZE + hdr.payload_length;
        pthread_mutex_unlock(&t->lock);

        switch (hdr.message_type) {
        case DAIM_PEER_MSG_STATE_SNAPSHOT_REQUEST:
            if (serve_snapshot_request(t, conn) != 0) {
                return -1;
            }
            fire_lifecycle(t, "snapshot_sent", conn->peer_node_id, 0);
            break;
        case DAIM_PEER_MSG_STATE_SNAPSHOT_ENTRY: {
            struct daim_host_location loc;
            enum daim_host_apply_result result;
            if (daim_peer_decode_host_location(payload, hdr.payload_length, &loc) != 0) {
                break;
            }
            result = daim_host_import_snapshot_entry(&loc);
            if (t->callbacks.on_apply) {
                t->callbacks.on_apply(t->callbacks.context, hdr.origin_node_id, &loc, result);
            }
            applied++;
            pthread_mutex_lock(&t->lock);
            t->stats.snapshot_entries_received++;
            pthread_mutex_unlock(&t->lock);
            break;
        }
        case DAIM_PEER_MSG_STATE_SNAPSHOT_END:
            my_snapshot_done = 1;
            break;
        default:
            /* Any other message type arriving mid-setup is unexpected
               here and is dropped defensively; the steady-state loop
               handles the full message set. */
            break;
        }
    }
    return applied;
}

/* --- steady-state loop, shared by dialer and acceptor once handshake +
   snapshot exchange is done --- */

static void handle_message(struct daim_peer_transport *t, struct connection *conn,
                            const struct daim_peer_header *hdr, const uint8_t *payload)
{
    switch (hdr->message_type) {
    case DAIM_PEER_MSG_HOST_LOCATION_UPDATE: {
        struct daim_host_location loc;
        enum daim_host_apply_result result;
        if (daim_peer_decode_host_location(payload, hdr->payload_length, &loc) != 0) {
            return;
        }
        result = daim_host_apply_remote(&loc);
        if (t->callbacks.on_apply) {
            t->callbacks.on_apply(t->callbacks.context, hdr->origin_node_id, &loc, result);
        }
        break;
    }
    case DAIM_PEER_MSG_STATE_SNAPSHOT_REQUEST:
        /* Defensive: a peer may re-request (e.g. after its own restart)
           outside the initial handshake phase. */
        serve_snapshot_request(t, conn);
        break;
    case DAIM_PEER_MSG_HEARTBEAT:
    case DAIM_PEER_MSG_ACK:
    case DAIM_PEER_MSG_ERROR:
    default:
        /* Not required for the prototype gate; decoded successfully by
           daim_peer_decode_header's framing but otherwise ignored. */
        break;
    }
}

static long acquire_connection_slot(struct daim_peer_transport *t, int fd)
{
    long i;
    pthread_mutex_lock(&t->lock);
    for (i = 0; i < MAX_CONNECTIONS; ++i) {
        if (!t->conns[i].active) {
            t->conns[i].fd = fd;
            t->conns[i].active = 1;
            t->conns[i].peer_node_id = 0;
            pthread_mutex_unlock(&t->lock);
            return i;
        }
    }
    pthread_mutex_unlock(&t->lock);
    return -1;
}

/* Runs one connection's full lifecycle (handshake, snapshot, steady-state
   read loop) until it drops. Blocks the calling thread for that whole
   lifetime; the caller (dial loop or accept handler) owns retry/exit. */
static void run_connection(struct daim_peer_transport *t, int fd, int is_outbound)
{
    long slot = acquire_connection_slot(t, fd);
    struct connection *conn;
    uint8_t hello_payload[8];
    int nodelay = 1;
    uint64_t disconnect_reason = 0;
    int disconnect_errno = 0;

    if (slot < 0) {
        close(fd);
        return;
    }
    conn = &t->conns[slot];
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#ifdef TCP_USER_TIMEOUT
    /* No application-level heartbeat in this milestone (see the header's
       "Not required for the prototype gate" note on DAIM_PEER_MSG_
       HEARTBEAT): without this, an idle connection cut by a network
       partition (e.g. an iptables DROP for the E6/G6 gate) would not be
       detected until Linux's default TCP retransmission timeout, which is
       minutes, not the seconds a gate run needs. This bounds how long
       unacknowledged data may sit on the socket before the kernel gives up
       and fails it, without requiring a wire-protocol heartbeat message. */
    {
        unsigned timeout_ms = UNRESPONSIVE_PEER_TIMEOUT_MS;
        setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout_ms, sizeof(timeout_ms));
    }
#endif

    daim_peer_encode_hello(hello_payload, t->owner_epoch);
    if (send_framed(t, conn, DAIM_PEER_MSG_HELLO, hello_payload, sizeof(hello_payload)) != 0) {
        disconnect_reason = 1; /* HELLO send failure */
        disconnect_errno = errno;
        goto done;
    }
    {
        uint8_t payload[DAIM_PEER_MAX_PAYLOAD_SIZE];
        struct daim_peer_header hdr;
        if (recv_framed(fd, &hdr, payload, sizeof(payload)) != 0 || hdr.message_type != DAIM_PEER_MSG_HELLO) {
            disconnect_reason = 2; /* HELLO receive/protocol failure */
            disconnect_errno = errno;
            goto done;
        }
        conn->peer_node_id = hdr.origin_node_id;
    }
    fire_lifecycle(t, "connected", conn->peer_node_id, (uint64_t)is_outbound);

    {
        int applied = exchange_snapshots(t, conn);
        if (applied < 0) {
            disconnect_reason = 3; /* snapshot exchange failure */
            disconnect_errno = errno;
            goto done;
        }
        fire_lifecycle(t, "snapshot_received", conn->peer_node_id, (uint64_t)applied);
    }

    for (;;) {
        uint8_t payload[DAIM_PEER_MAX_PAYLOAD_SIZE];
        struct daim_peer_header hdr;
        if (should_stop(t)) {
            break;
        }
        if (recv_framed(fd, &hdr, payload, sizeof(payload)) != 0) {
            disconnect_reason = 4; /* steady-state receive/framing failure */
            disconnect_errno = errno;
            break;
        }
        pthread_mutex_lock(&t->lock);
        t->stats.messages_received++;
        t->stats.bytes_received += DAIM_PEER_HEADER_SIZE + hdr.payload_length;
        pthread_mutex_unlock(&t->lock);
        handle_message(t, conn, &hdr, payload);
    }

done:
    /* Emit both a stable reason code and the captured errno.  EOF and clean
       shutdown legitimately have errno 0, so errno alone is ambiguous. */
    fire_lifecycle(t, "disconnect_reason", conn->peer_node_id, disconnect_reason);
    fire_lifecycle(t, "disconnected", conn->peer_node_id, (uint64_t)disconnect_errno);
    pthread_mutex_lock(&conn->send_lock);
    shutdown(fd, SHUT_RDWR);
    close(fd);
    pthread_mutex_lock(&t->lock);
    conn->active = 0;
    pthread_mutex_unlock(&t->lock);
    pthread_mutex_unlock(&conn->send_lock);
}

static void *dial_thread_main(void *arg)
{
    struct dial_thread_arg *a = arg;
    struct daim_peer_transport *t = a->t;
    struct configured_peer peer = t->peers[a->peer_index];
    int backoff_ms = BACKOFF_BASE_MS;
    unsigned attempt = 0;
    free(a);

    while (!should_stop(t)) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr;
        int rc;
        if (fd < 0) {
            break;
        }
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)peer.port);
        inet_pton(AF_INET, peer.host, &addr.sin_addr);

        rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
        if (rc == 0) {
            backoff_ms = BACKOFF_BASE_MS;
            attempt = 0;
            run_connection(t, fd, 1); /* blocks for the connection's lifetime */
            continue;
        }
        close(fd);
        pthread_mutex_lock(&t->lock);
        t->stats.reconnect_attempts++;
        pthread_mutex_unlock(&t->lock);
        attempt++;
        fire_lifecycle(t, "reconnecting", 0, attempt);

        {
            int waited = 0;
            while (waited < backoff_ms && !should_stop(t)) {
                sleep_ms(STOP_POLL_MS);
                waited += STOP_POLL_MS;
            }
        }
        backoff_ms *= 2;
        if (backoff_ms > BACKOFF_CAP_MS) {
            backoff_ms = BACKOFF_CAP_MS;
        }
    }
    return NULL;
}

static void *accept_conn_thread_main(void *arg)
{
    struct accept_thread_arg *a = arg;
    struct daim_peer_transport *t = a->t;
    int fd = a->fd;
    free(a);
    run_connection(t, fd, 0);
    pthread_mutex_lock(&t->lock);
    t->active_accept_handlers--;
    pthread_cond_broadcast(&t->accept_handlers_done);
    pthread_mutex_unlock(&t->lock);
    return NULL;
}

static void *accept_thread_main(void *arg)
{
    struct daim_peer_transport *t = arg;
    for (;;) {
        int fd = accept(t->listen_fd, NULL, NULL);
        struct accept_thread_arg *cta;
        pthread_t th;
        if (fd < 0) {
            break; /* listen_fd closed by destroy(), or a real error */
        }
        if (should_stop(t)) {
            close(fd);
            break;
        }
        cta = malloc(sizeof(*cta));
        if (!cta) {
            close(fd);
            continue;
        }
        cta->t = t;
        cta->fd = fd;
        pthread_mutex_lock(&t->lock);
        t->active_accept_handlers++;
        pthread_mutex_unlock(&t->lock);
        if (pthread_create(&th, NULL, accept_conn_thread_main, cta) != 0) {
            pthread_mutex_lock(&t->lock);
            t->active_accept_handlers--;
            pthread_cond_broadcast(&t->accept_handlers_done);
            pthread_mutex_unlock(&t->lock);
            free(cta);
            close(fd);
            continue;
        }
        /* Handler lifetimes are counted explicitly; detaching avoids an
           ever-growing join list across repeated reconnects. */
        pthread_detach(th);
    }
    return NULL;
}

struct daim_peer_transport *daim_peer_transport_create(
    uint64_t node_id, uint64_t owner_epoch, int listen_port,
    const struct daim_peer_transport_callbacks *callbacks)
{
    struct daim_peer_transport *t = calloc(1, sizeof(*t));
    struct sockaddr_in addr;
    int yes = 1;
    pthread_t accept_th;
    long i;

    if (!t) {
        return NULL;
    }
    /* Writing to a connection whose peer already closed (a killed node in
       G5, or a partitioned one in G6) raises SIGPIPE by default, whose
       default disposition terminates the whole process -- not just this
       send() call. This node's process must survive a dead/unreachable
       peer, so SIGPIPE is ignored process-wide; send_all() already reports
       the failure through its return value, which is what every caller
       here actually checks. Process-wide is intentional and safe: nothing
       else in this codebase relies on SIGPIPE's default action. */
    signal(SIGPIPE, SIG_IGN);
    pthread_mutex_init(&t->lock, NULL);
    pthread_cond_init(&t->accept_handlers_done, NULL);
    for (i = 0; i < MAX_CONNECTIONS; ++i) {
        pthread_mutex_init(&t->conns[i].send_lock, NULL);
    }
    t->node_id = node_id;
    t->owner_epoch = owner_epoch;
    if (callbacks) {
        t->callbacks = *callbacks;
    }

    t->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (t->listen_fd < 0) {
        for (i = 0; i < MAX_CONNECTIONS; ++i) {
            pthread_mutex_destroy(&t->conns[i].send_lock);
        }
        pthread_cond_destroy(&t->accept_handlers_done);
        pthread_mutex_destroy(&t->lock);
        free(t);
        return NULL;
    }
    setsockopt(t->listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)listen_port);
    if (bind(t->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(t->listen_fd, LISTEN_BACKLOG) != 0) {
        close(t->listen_fd);
        for (i = 0; i < MAX_CONNECTIONS; ++i) {
            pthread_mutex_destroy(&t->conns[i].send_lock);
        }
        pthread_cond_destroy(&t->accept_handlers_done);
        pthread_mutex_destroy(&t->lock);
        free(t);
        return NULL;
    }

    if (pthread_create(&accept_th, NULL, accept_thread_main, t) != 0) {
        close(t->listen_fd);
        for (i = 0; i < MAX_CONNECTIONS; ++i) {
            pthread_mutex_destroy(&t->conns[i].send_lock);
        }
        pthread_cond_destroy(&t->accept_handlers_done);
        pthread_mutex_destroy(&t->lock);
        free(t);
        return NULL;
    }
    register_thread(t, accept_th);
    return t;
}

int daim_peer_transport_add_peer(struct daim_peer_transport *t, const char *host, int port)
{
    struct dial_thread_arg *arg;
    pthread_t th;
    size_t idx;

    if (!t || !host) {
        return -1;
    }
    pthread_mutex_lock(&t->lock);
    if (t->peer_count >= MAX_CONFIGURED_PEERS) {
        pthread_mutex_unlock(&t->lock);
        return -1;
    }
    idx = t->peer_count++;
    snprintf(t->peers[idx].host, sizeof(t->peers[idx].host), "%s", host);
    t->peers[idx].port = port;
    t->stats.configured_peers = t->peer_count;
    pthread_mutex_unlock(&t->lock);

    arg = malloc(sizeof(*arg));
    if (!arg) {
        return -1;
    }
    arg->t = t;
    arg->peer_index = idx;
    if (pthread_create(&th, NULL, dial_thread_main, arg) != 0) {
        free(arg);
        return -1;
    }
    register_thread(t, th);
    return 0;
}

int daim_peer_transport_disseminate(struct daim_peer_transport *t, const struct daim_host_location *loc)
{
    uint8_t buf[DAIM_PEER_HOST_LOCATION_WIRE_SIZE];
    int sent_count = 0;
    long i;
    struct connection *snapshot[MAX_CONNECTIONS];
    long snapshot_count = 0;

    if (!t || !loc) {
        return 0;
    }
    daim_peer_encode_host_location(buf, loc);

    pthread_mutex_lock(&t->lock);
    for (i = 0; i < MAX_CONNECTIONS; ++i) {
        if (t->conns[i].active) {
            snapshot[snapshot_count++] = &t->conns[i];
        }
    }
    pthread_mutex_unlock(&t->lock);

    for (i = 0; i < snapshot_count; ++i) {
        if (send_framed(t, snapshot[i], DAIM_PEER_MSG_HOST_LOCATION_UPDATE, buf, sizeof(buf)) == 0) {
            sent_count++;
        }
    }
    return sent_count;
}

void daim_peer_transport_get_stats(struct daim_peer_transport *t, struct daim_peer_transport_stats *out)
{
    long i;
    if (!t || !out) {
        return;
    }
    pthread_mutex_lock(&t->lock);
    *out = t->stats;
    out->active_connections = 0;
    for (i = 0; i < MAX_CONNECTIONS; ++i) {
        if (t->conns[i].active) {
            out->active_connections++;
        }
    }
    pthread_mutex_unlock(&t->lock);
}

void daim_peer_transport_destroy(struct daim_peer_transport *t)
{
    long i;
    size_t j;
    pthread_t threads_copy[MAX_THREADS];
    size_t thread_count_copy;

    if (!t) {
        return;
    }
    request_stop(t);

    shutdown(t->listen_fd, SHUT_RDWR);
    close(t->listen_fd);

    pthread_mutex_lock(&t->lock);
    for (i = 0; i < MAX_CONNECTIONS; ++i) {
        if (t->conns[i].active) {
            shutdown(t->conns[i].fd, SHUT_RDWR);
        }
    }
    thread_count_copy = t->thread_count;
    memcpy(threads_copy, t->threads, thread_count_copy * sizeof(pthread_t));
    pthread_mutex_unlock(&t->lock);

    for (j = 0; j < thread_count_copy; ++j) {
        pthread_join(threads_copy[j], NULL);
    }

    pthread_mutex_lock(&t->lock);
    while (t->active_accept_handlers > 0) {
        pthread_cond_wait(&t->accept_handlers_done, &t->lock);
    }
    pthread_mutex_unlock(&t->lock);

    for (i = 0; i < MAX_CONNECTIONS; ++i) {
        pthread_mutex_destroy(&t->conns[i].send_lock);
    }
    pthread_cond_destroy(&t->accept_handlers_done);
    pthread_mutex_destroy(&t->lock);
    free(t);
}
