#define _POSIX_C_SOURCE 200809L

#include "ovs_persistent_adapter.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

/* --- OpenFlow 1.3 wire constants (subset actually used here) --- */
#define OFP_VERSION_13 0x04

#define OFPT_HELLO 0
#define OFPT_ECHO_REQUEST 2
#define OFPT_ECHO_REPLY 3
#define OFPT_FLOW_MOD 14

#define OFPFC_ADD 0
#define OFPFC_DELETE 3

#define OFPP_NORMAL 0xfffffffaU
#define OFPP_ANY 0xffffffffU
#define OFPG_ANY 0xffffffffU
#define OFP_NO_BUFFER 0xffffffffU

#define OFPMT_OXM 1
#define OFPXMC_OPENFLOW_BASIC 0x8000u
#define OXM_OF_IN_PORT 0
#define OXM_OF_ETH_DST 3
#define OXM_OF_ETH_TYPE 5

#define OFPIT_APPLY_ACTIONS 4
#define OFPAT_OUTPUT 0
#define OFPCML_NO_BUFFER 0xffff

#define ETH_TYPE_IPV4 0x0800

struct persistent_context {
    pthread_mutex_t lock;
    int listen_fd;
    int conn_fd;
    int connected;
    pthread_t reader_thread;
    int reader_running;
    uint32_t next_xid;
    struct daim_persistent_adapter_stats stats;
};

/* --- byte buffer helpers (network byte order) --- */

static void put_u8(uint8_t *buf, size_t *off, uint8_t v) { buf[(*off)++] = v; }
static void put_u16(uint8_t *buf, size_t *off, uint16_t v) {
    uint16_t n = htons(v);
    memcpy(buf + *off, &n, 2);
    *off += 2;
}
static void put_u32(uint8_t *buf, size_t *off, uint32_t v) {
    uint32_t n = htonl(v);
    memcpy(buf + *off, &n, 4);
    *off += 4;
}
static void put_u64(uint8_t *buf, size_t *off, uint64_t v) {
    uint32_t hi = htonl((uint32_t)(v >> 32));
    uint32_t lo = htonl((uint32_t)(v & 0xffffffffU));
    memcpy(buf + *off, &hi, 4);
    memcpy(buf + *off + 4, &lo, 4);
    *off += 8;
}
static void put_bytes(uint8_t *buf, size_t *off, const void *src, size_t n) {
    memcpy(buf + *off, src, n);
    *off += n;
}
static void put_zero(uint8_t *buf, size_t *off, size_t n) {
    memset(buf + *off, 0, n);
    *off += n;
}
static size_t pad8(size_t len) { return (len + 7u) & ~((size_t)7u); }

/* --- flow-string parsing for the subset used in this codebase --- */

struct parsed_flow {
    int has_priority;
    uint16_t priority;
    int has_in_port;
    uint32_t in_port;
    int has_dl_dst;
    uint8_t dl_dst[6];
    int has_ip;
    int has_output;
    uint32_t output_port; /* or OFPP_NORMAL */
};

static int parse_mac(const char *s, uint8_t mac[6]) {
    unsigned int b[6];
    if (sscanf(s, "%2x:%2x:%2x:%2x:%2x:%2x", &b[0], &b[1], &b[2], &b[3], &b[4], &b[5]) != 6) {
        return -1;
    }
    for (int i = 0; i < 6; i++) mac[i] = (uint8_t)b[i];
    return 0;
}

static int parse_flow_string(const char *flow, struct parsed_flow *out) {
    char buf[512];
    memset(out, 0, sizeof(*out));
    if (!flow || strlen(flow) >= sizeof(buf)) return -1;
    strcpy(buf, flow);

    char *save = NULL;
    for (char *tok = strtok_r(buf, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
        if (strncmp(tok, "priority=", 9) == 0) {
            out->has_priority = 1;
            out->priority = (uint16_t)atoi(tok + 9);
        } else if (strncmp(tok, "in_port=", 8) == 0) {
            out->has_in_port = 1;
            out->in_port = (uint32_t)atoi(tok + 8);
        } else if (strncmp(tok, "dl_dst=", 7) == 0) {
            if (parse_mac(tok + 7, out->dl_dst) != 0) return -1;
            out->has_dl_dst = 1;
        } else if (strcmp(tok, "ip") == 0) {
            out->has_ip = 1;
        } else if (strncmp(tok, "actions=output:", 15) == 0) {
            out->has_output = 1;
            out->output_port = (uint32_t)atoi(tok + 15);
        } else if (strcmp(tok, "actions=normal") == 0) {
            out->has_output = 1;
            out->output_port = OFPP_NORMAL;
        }
        /* Unrecognised tokens (e.g. bare match fields for delete) are
           ignored here; delete uses the has_* flags directly. */
    }
    return 0;
}

/* --- OpenFlow message construction --- */

static size_t build_match(uint8_t *buf, const struct parsed_flow *f) {
    size_t off = 0;
    size_t match_start = off;
    put_u16(buf, &off, OFPMT_OXM);
    size_t len_pos = off;
    off += 2; /* length filled in later */

    if (f->has_in_port) {
        put_u32(buf, &off, (OFPXMC_OPENFLOW_BASIC << 16) | (OXM_OF_IN_PORT << 9) | 4);
        put_u32(buf, &off, f->in_port);
    }
    if (f->has_dl_dst) {
        put_u32(buf, &off, (OFPXMC_OPENFLOW_BASIC << 16) | (OXM_OF_ETH_DST << 9) | 6);
        put_bytes(buf, &off, f->dl_dst, 6);
    }
    if (f->has_ip) {
        put_u32(buf, &off, (OFPXMC_OPENFLOW_BASIC << 16) | (OXM_OF_ETH_TYPE << 9) | 2);
        put_u16(buf, &off, ETH_TYPE_IPV4);
    }

    size_t match_len = off - match_start;
    uint16_t match_len_be = htons((uint16_t)match_len);
    memcpy(buf + len_pos, &match_len_be, 2);

    size_t padded = pad8(match_len);
    while (off < match_start + padded) buf[off++] = 0;
    return off - match_start;
}

static size_t build_actions(uint8_t *buf, const struct parsed_flow *f) {
    if (!f->has_output) return 0;
    size_t off = 0;
    /* ofp_instruction_actions header */
    put_u16(buf, &off, OFPIT_APPLY_ACTIONS);
    size_t len_pos = off;
    off += 2;
    put_zero(buf, &off, 4);
    /* ofp_action_output */
    put_u16(buf, &off, OFPAT_OUTPUT);
    put_u16(buf, &off, 16);
    put_u32(buf, &off, f->output_port);
    put_u16(buf, &off, OFPCML_NO_BUFFER);
    put_zero(buf, &off, 6);

    uint16_t len_be = htons((uint16_t)off);
    memcpy(buf + len_pos, &len_be, 2);
    return off;
}

/* Builds a full OFPT_FLOW_MOD message into buf (must be large enough,
   256 bytes is ample for this subset). Returns the message length. */
static size_t build_flow_mod(uint8_t *buf, uint32_t xid, uint8_t command,
                             const struct parsed_flow *f) {
    size_t off = 0;
    size_t header_pos = off;
    put_u8(buf, &off, OFP_VERSION_13);
    put_u8(buf, &off, OFPT_FLOW_MOD);
    off += 2; /* total length filled in below via header_pos */
    put_u32(buf, &off, xid);

    put_u64(buf, &off, 0);            /* cookie */
    put_u64(buf, &off, 0);            /* cookie_mask */
    put_u8(buf, &off, 0);             /* table_id */
    put_u8(buf, &off, command);
    put_u16(buf, &off, 0);            /* idle_timeout */
    put_u16(buf, &off, 0);            /* hard_timeout */
    put_u16(buf, &off, f->has_priority ? f->priority : 0);
    put_u32(buf, &off, OFP_NO_BUFFER);
    put_u32(buf, &off, OFPP_ANY);
    put_u32(buf, &off, OFPG_ANY);
    put_u16(buf, &off, 0);            /* flags */
    put_zero(buf, &off, 2);

    uint8_t match_buf[128];
    size_t match_len = build_match(match_buf, f);
    put_bytes(buf, &off, match_buf, match_len);

    if (command == OFPFC_ADD) {
        uint8_t action_buf[64];
        size_t action_len = build_actions(action_buf, f);
        put_bytes(buf, &off, action_buf, action_len);
    }

    uint16_t total_len_be = htons((uint16_t)off);
    memcpy(buf + header_pos + 2, &total_len_be, 2);
    return off;
}

static size_t build_hello(uint8_t *buf, uint32_t xid) {
    size_t off = 0;
    put_u8(buf, &off, OFP_VERSION_13);
    put_u8(buf, &off, OFPT_HELLO);
    put_u16(buf, &off, 8);
    put_u32(buf, &off, xid);
    return off;
}

static size_t build_echo_reply(uint8_t *buf, uint32_t xid) {
    size_t off = 0;
    put_u8(buf, &off, OFP_VERSION_13);
    put_u8(buf, &off, OFPT_ECHO_REPLY);
    put_u16(buf, &off, 8);
    put_u32(buf, &off, xid);
    return off;
}

/* --- connection handling --- */

static void *reader_loop(void *arg) {
    struct daim_switch_adapter *adapter = arg;
    struct persistent_context *ctx = adapter->context;
    uint8_t header[8];
    uint8_t body[2048];

    while (1) {
        ssize_t n = recv(ctx->conn_fd, header, 8, MSG_WAITALL);
        if (n != 8) break;
        uint16_t len = ntohs(*(uint16_t *)(header + 2));
        uint32_t xid = ntohl(*(uint32_t *)(header + 4));
        size_t remaining = len > 8 ? (size_t)(len - 8) : 0;
        if (remaining > sizeof(body)) remaining = sizeof(body);
        if (remaining > 0) {
            if (recv(ctx->conn_fd, body, remaining, MSG_WAITALL) <= 0) break;
        }
        if (header[1] == OFPT_ECHO_REQUEST) {
            uint8_t reply[8];
            size_t rlen = build_echo_reply(reply, xid);
            pthread_mutex_lock(&ctx->lock);
            if (send(ctx->conn_fd, reply, rlen, 0) == (ssize_t)rlen) {
                ctx->stats.echo_replies_sent++;
            }
            pthread_mutex_unlock(&ctx->lock);
        }
        /* Other message types (Packet-In, Port-Status, ...) are drained
           and discarded; this adapter only issues Flow-Mods. */
    }
    return NULL;
}

static int do_handshake(int fd, int timeout_seconds) {
    struct timeval tv = {timeout_seconds, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    uint8_t hello[8];
    size_t hlen = build_hello(hello, 1);
    if (send(fd, hello, hlen, 0) != (ssize_t)hlen) return -1;

    uint8_t header[8];
    if (recv(fd, header, 8, MSG_WAITALL) != 8) return -1;
    uint16_t len = ntohs(*(uint16_t *)(header + 2));
    if (len > 8) {
        uint8_t discard[256];
        size_t remaining = len - 8;
        if (remaining > sizeof(discard)) remaining = sizeof(discard);
        recv(fd, discard, remaining, MSG_WAITALL);
    }

    struct timeval no_timeout = {0, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &no_timeout, sizeof(no_timeout));
    return 0;
}

/* --- daim_switch_adapter_ops implementation --- */

static int persistent_flow_add(struct daim_switch_adapter *adapter, const char *bridge, const char *flow) {
    (void)bridge; /* one persistent connection is bound to one bridge already */
    struct persistent_context *ctx = adapter->context;
    struct parsed_flow f;
    if (parse_flow_string(flow, &f) != 0) return -1;

    uint8_t msg[256];
    pthread_mutex_lock(&ctx->lock);
    size_t len = build_flow_mod(msg, ctx->next_xid++, OFPFC_ADD, &f);
    ssize_t sent = send(ctx->conn_fd, msg, len, 0);
    if (sent == (ssize_t)len) {
        ctx->stats.flow_mods_sent++;
        ctx->stats.bytes_sent += len;
    }
    pthread_mutex_unlock(&ctx->lock);
    return sent == (ssize_t)len ? 0 : -1;
}

static int persistent_flow_delete(struct daim_switch_adapter *adapter, const char *bridge, const char *flow_match) {
    (void)bridge;
    struct persistent_context *ctx = adapter->context;
    struct parsed_flow f;
    if (parse_flow_string(flow_match, &f) != 0) return -1;

    uint8_t msg[256];
    pthread_mutex_lock(&ctx->lock);
    size_t len = build_flow_mod(msg, ctx->next_xid++, OFPFC_DELETE, &f);
    ssize_t sent = send(ctx->conn_fd, msg, len, 0);
    if (sent == (ssize_t)len) {
        ctx->stats.flow_mods_sent++;
        ctx->stats.bytes_sent += len;
    }
    pthread_mutex_unlock(&ctx->lock);
    return sent == (ssize_t)len ? 0 : -1;
}

static int persistent_unsupported_read(struct daim_switch_adapter *a, const uint8_t m[6], void *b, uint64_t s) { (void)a;(void)m;(void)b;(void)s; return -1; }
static int persistent_unsupported_write(struct daim_switch_adapter *a, const uint8_t m[6], const void *b, uint64_t s) { (void)a;(void)m;(void)b;(void)s; return -1; }
static int persistent_unsupported_ioctl(struct daim_switch_adapter *a, uint64_t r, void *d) { (void)a;(void)r;(void)d; return -1; }

static void persistent_destroy(struct daim_switch_adapter *adapter) {
    struct persistent_context *ctx = adapter->context;
    if (!ctx) return;
    if (ctx->conn_fd >= 0) shutdown(ctx->conn_fd, SHUT_RDWR);
    if (ctx->reader_running) pthread_join(ctx->reader_thread, NULL);
    if (ctx->conn_fd >= 0) close(ctx->conn_fd);
    if (ctx->listen_fd >= 0) close(ctx->listen_fd);
    pthread_mutex_destroy(&ctx->lock);
    free(ctx);
    adapter->context = NULL;
    adapter->ops = NULL;
}

static const struct daim_switch_adapter_ops persistent_ops = {
    persistent_unsupported_read,
    persistent_unsupported_write,
    persistent_unsupported_ioctl,
    persistent_flow_add,
    persistent_flow_delete,
    persistent_destroy,
};

int daim_ovs_persistent_adapter_create(struct daim_switch_adapter *adapter,
                                       int listen_port, int timeout_seconds) {
    if (!adapter) return -1;

    struct persistent_context *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return -1;
    pthread_mutex_init(&ctx->lock, NULL);
    ctx->next_xid = 100;
    ctx->listen_fd = -1;
    ctx->conn_fd = -1;

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { free(ctx); return -1; }
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)listen_port);
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(listen_fd); free(ctx); return -1;
    }
    if (listen(listen_fd, 1) != 0) {
        close(listen_fd); free(ctx); return -1;
    }

    struct timeval tv = {timeout_seconds, 0};
    setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    int conn_fd = accept(listen_fd, NULL, NULL);
    if (conn_fd < 0) {
        close(listen_fd); free(ctx); return -1;
    }
    int nodelay = 1;
    setsockopt(conn_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    if (do_handshake(conn_fd, timeout_seconds) != 0) {
        close(conn_fd); close(listen_fd); free(ctx); return -1;
    }

    ctx->listen_fd = listen_fd;
    ctx->conn_fd = conn_fd;
    ctx->connected = 1;

    adapter->ops = &persistent_ops;
    adapter->context = ctx;

    ctx->reader_running = 1;
    if (pthread_create(&ctx->reader_thread, NULL, reader_loop, adapter) != 0) {
        ctx->reader_running = 0;
    }

    return 0;
}

int daim_ovs_persistent_adapter_get_stats(struct daim_switch_adapter *adapter,
                                          struct daim_persistent_adapter_stats *stats) {
    if (!adapter || !adapter->context || !stats) return -1;
    struct persistent_context *ctx = adapter->context;
    pthread_mutex_lock(&ctx->lock);
    *stats = ctx->stats;
    pthread_mutex_unlock(&ctx->lock);
    return 0;
}

size_t daim_ovs_wire_flow_mod_size(const char *flow, int is_delete) {
    struct parsed_flow f;
    if (parse_flow_string(flow, &f) != 0) return 0;
    uint8_t scratch[256];
    return build_flow_mod(scratch, 0, is_delete ? OFPFC_DELETE : OFPFC_ADD, &f);
}
