#define _POSIX_C_SOURCE 200809L

/* Wire-level unit test for daim_peer_transport.c: connection setup,
   snapshot exchange, and live dissemination between two transport
   instances in one process over 127.0.0.1.

   daim_distributed_state.c is a per-process singleton (like
   daim_learning_app.c's g_app -- one node per process, by design), so a
   single test binary cannot host two independent nodes' state tables to
   verify cross-node convergence; that is exercised for real by the
   multi-process E1-E6 gate run (distributed_prototype_gate.py). This test
   verifies the transport's own responsibility: that a message sent on one
   transport is correctly framed, received, decoded, and handed to the
   apply path on the other end. */
#include "daim_distributed_state.h"
#include "daim_peer_transport.h"

#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define TRANSPORT_A_PORT 19100
#define TRANSPORT_B_PORT 19101
#define TRANSPORT_C_PORT 19102

struct event_log {
    pthread_mutex_t lock;
    int applied_count;
    uint64_t last_applied_origin;
    uint8_t last_applied_mac[MAC_ADDR_LEN];
    enum daim_host_apply_result last_result;
    int connected_count;
    int snapshot_events;
};

struct sender_arg {
    struct daim_peer_transport *transport;
    struct daim_host_location location;
    int sends;
};

static struct event_log g_log_a;
static struct event_log g_log_b;

static void on_apply(void *context, uint64_t from_peer_node_id,
                      const struct daim_host_location *loc, enum daim_host_apply_result result)
{
    struct event_log *log = context;
    (void)from_peer_node_id;
    pthread_mutex_lock(&log->lock);
    log->applied_count++;
    log->last_applied_origin = loc->origin_node_id;
    memcpy(log->last_applied_mac, loc->mac, MAC_ADDR_LEN);
    log->last_result = result;
    pthread_mutex_unlock(&log->lock);
}

static void on_lifecycle(void *context, const char *event_name, uint64_t peer_node_id, uint64_t detail)
{
    struct event_log *log = context;
    (void)peer_node_id;
    (void)detail;
    pthread_mutex_lock(&log->lock);
    if (strcmp(event_name, "connected") == 0) {
        log->connected_count++;
    } else if (strcmp(event_name, "snapshot_received") == 0 || strcmp(event_name, "snapshot_sent") == 0) {
        log->snapshot_events++;
    }
    pthread_mutex_unlock(&log->lock);
}

static int wait_until(int (*predicate)(void), int timeout_ms)
{
    int waited = 0;
    struct timespec pause = {0, 5000000L}; /* 5ms; usleep is not in POSIX.1-2008 */
    while (waited < timeout_ms) {
        if (predicate()) {
            return 1;
        }
        nanosleep(&pause, NULL);
        waited += 5;
    }
    return 0;
}

static int a_connected(void)
{
    int v;
    pthread_mutex_lock(&g_log_a.lock);
    v = g_log_a.connected_count;
    pthread_mutex_unlock(&g_log_a.lock);
    return v >= 1;
}

static int b_connected(void)
{
    int v;
    pthread_mutex_lock(&g_log_b.lock);
    v = g_log_b.connected_count;
    pthread_mutex_unlock(&g_log_b.lock);
    return v >= 1;
}

static int b_applied(void)
{
    int v;
    pthread_mutex_lock(&g_log_b.lock);
    v = g_log_b.applied_count;
    pthread_mutex_unlock(&g_log_b.lock);
    return v >= 1;
}

static void *concurrent_sender(void *context)
{
    struct sender_arg *arg = context;
    int i;
    for (i = 0; i < arg->sends; ++i) {
        assert(daim_peer_transport_disseminate(arg->transport, &arg->location) == 1);
    }
    return NULL;
}

int main(void)
{
    struct daim_peer_transport_callbacks cb_a = {on_apply, on_lifecycle, &g_log_a};
    struct daim_peer_transport_callbacks cb_b = {on_apply, on_lifecycle, &g_log_b};
    struct daim_peer_transport *ta, *tb;
    struct daim_host_location loc;
    struct daim_peer_transport_stats stats_a, stats_b;

    pthread_mutex_init(&g_log_a.lock, NULL);
    pthread_mutex_init(&g_log_b.lock, NULL);

    /* Single shared table for this process; node_id=99 is neither peer's
       origin, so neither side's disseminated records collide with "this
       is my own state, reject the remote claim" (see daim_distributed_
       state.c's apply_locked). */
    assert(daim_host_state_init(99, 1) == 0);

    ta = daim_peer_transport_create(1, 1, TRANSPORT_A_PORT, &cb_a);
    assert(ta != NULL);
    tb = daim_peer_transport_create(2, 1, TRANSPORT_B_PORT, &cb_b);
    assert(tb != NULL);

    assert(daim_peer_transport_add_peer(ta, "127.0.0.1", TRANSPORT_B_PORT) == 0);

    assert(wait_until(a_connected, 2000));
    assert(wait_until(b_connected, 2000));

    daim_peer_transport_get_stats(ta, &stats_a);
    daim_peer_transport_get_stats(tb, &stats_b);
    assert(stats_a.active_connections == 1);
    assert(stats_b.active_connections == 1);
    assert(stats_a.configured_peers == 1);

    /* Live dissemination: A sends a record it did not necessarily learn
       itself (constructed directly, exercising the transport's own
       responsibility rather than daim_host_learn_local's). */
    memset(&loc, 0, sizeof(loc));
    loc.mac[0] = 0x00; loc.mac[5] = 0x04;
    loc.origin_node_id = 1;
    loc.owner_dpid = 101;
    loc.owner_port = 3;
    loc.owner_epoch = 1;
    loc.sequence = 1;
    loc.is_local = 1;
    loc.learned_at_ns = 12345;

    assert(daim_peer_transport_disseminate(ta, &loc) == 1);
    assert(wait_until(b_applied, 2000));

    pthread_mutex_lock(&g_log_b.lock);
    assert(g_log_b.last_applied_origin == 1);
    assert(memcmp(g_log_b.last_applied_mac, loc.mac, MAC_ADDR_LEN) == 0);
    assert(g_log_b.last_result == DAIM_HOST_APPLY_NEW || g_log_b.last_result == DAIM_HOST_APPLY_UPDATED);
    pthread_mutex_unlock(&g_log_b.lock);

    /* Multiple application threads may disseminate concurrently on the
       same TCP connection. Frames must remain serialized and decodable. */
    {
        enum { SENDERS = 4, SENDS_PER_THREAD = 25 };
        pthread_t senders[SENDERS];
        struct sender_arg args[SENDERS];
        int i;
        for (i = 0; i < SENDERS; ++i) {
            args[i].transport = ta;
            args[i].location = loc;
            args[i].location.sequence = (uint64_t)(1000 + i);
            args[i].sends = SENDS_PER_THREAD;
            assert(pthread_create(&senders[i], NULL, concurrent_sender, &args[i]) == 0);
        }
        for (i = 0; i < SENDERS; ++i) {
            assert(pthread_join(senders[i], NULL) == 0);
        }
        /* Give the receiver time to decode all frames before teardown. */
        {
            struct timespec pause = {0, 100000000L};
            nanosleep(&pause, NULL);
        }
        pthread_mutex_lock(&g_log_b.lock);
        assert(g_log_b.applied_count >= 1 + SENDERS * SENDS_PER_THREAD);
        pthread_mutex_unlock(&g_log_b.lock);
    }

    /* The record disseminated over the wire is now visible through the
       (shared, in this single-process test) state table's own lookup. */
    {
        struct daim_host_location out;
        assert(daim_host_lookup(loc.mac, &out) == 1);
        assert(out.owner_dpid == 101);
    }

    daim_peer_transport_get_stats(ta, &stats_a);
    assert(stats_a.messages_sent >= 2); /* HELLO + the disseminated update, at least */

    daim_peer_transport_destroy(ta);
    daim_peer_transport_destroy(tb);

    /* Peer-count boundary: MAX_CONFIGURED_PEERS is 31, sized for a 32-node
       full-mesh cluster (node 1 dials all 31 higher-numbered peers).
       Configuring up to the cap must succeed; the next call must fail
       cleanly (-1, no crash) rather than overflowing peers[]. Targets need
       not be reachable -- this only exercises add_peer's own bookkeeping. */
    {
        struct daim_peer_transport *tc;
        struct daim_peer_transport_stats stats_c;
        int i;

        tc = daim_peer_transport_create(3, 1, TRANSPORT_C_PORT, &cb_a);
        assert(tc != NULL);

        for (i = 0; i < 31; ++i) {
            assert(daim_peer_transport_add_peer(tc, "127.0.0.1", 20000 + i) == 0);
        }
        assert(daim_peer_transport_add_peer(tc, "127.0.0.1", 20031) == -1);

        daim_peer_transport_get_stats(tc, &stats_c);
        assert(stats_c.configured_peers == 31);

        daim_peer_transport_destroy(tc);
    }

    return 0;
}
