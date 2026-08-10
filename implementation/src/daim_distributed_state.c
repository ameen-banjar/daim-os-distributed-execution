#define _POSIX_C_SOURCE 200809L

#include "daim_distributed_state.h"

#include <pthread.h>
#include <string.h>
#include <time.h>

#define MAX_HOST_ENTRIES 4096

struct state {
    pthread_mutex_t lock;
    int ready;
    uint64_t node_id;
    uint64_t owner_epoch;
    uint64_t next_local_sequence;
    struct daim_host_location table[MAX_HOST_ENTRIES];
    size_t count;
    struct daim_host_state_stats stats;
};

static struct state g_state;

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int mac_eq(const uint8_t a[MAC_ADDR_LEN], const uint8_t b[MAC_ADDR_LEN])
{
    return memcmp(a, b, MAC_ADDR_LEN) == 0;
}

/* Caller must hold g_state.lock. Returns the index of an existing record
   for this MAC, or -1 if none exists. */
static long find_index(const uint8_t mac[MAC_ADDR_LEN])
{
    size_t i;
    for (i = 0; i < g_state.count; ++i) {
        if (mac_eq(g_state.table[i].mac, mac)) {
            return (long)i;
        }
    }
    return -1;
}

int daim_host_state_init(uint64_t node_id, uint64_t owner_epoch)
{
    if (!g_state.ready) {
        if (pthread_mutex_init(&g_state.lock, NULL) != 0) {
            return -1;
        }
        g_state.ready = 1;
    }
    pthread_mutex_lock(&g_state.lock);
    g_state.node_id = node_id;
    g_state.owner_epoch = owner_epoch;
    g_state.next_local_sequence = 1;
    g_state.count = 0;
    memset(&g_state.stats, 0, sizeof(g_state.stats));
    pthread_mutex_unlock(&g_state.lock);
    return 0;
}

uint64_t daim_host_learn_local(const uint8_t mac[MAC_ADDR_LEN], uint64_t dpid, uint32_t port)
{
    uint64_t sequence;
    long idx;
    if (!g_state.ready || !mac) {
        return 0;
    }
    pthread_mutex_lock(&g_state.lock);
    sequence = g_state.next_local_sequence++;
    idx = find_index(mac);
    if (idx < 0) {
        if (g_state.count >= MAX_HOST_ENTRIES) {
            pthread_mutex_unlock(&g_state.lock);
            return 0;
        }
        idx = (long)g_state.count++;
    }
    memset(&g_state.table[idx], 0, sizeof(g_state.table[idx]));
    memcpy(g_state.table[idx].mac, mac, MAC_ADDR_LEN);
    g_state.table[idx].origin_node_id = g_state.node_id;
    g_state.table[idx].owner_dpid = dpid;
    g_state.table[idx].owner_port = port;
    g_state.table[idx].owner_epoch = g_state.owner_epoch;
    g_state.table[idx].sequence = sequence;
    g_state.table[idx].is_local = 1;
    g_state.table[idx].learned_at_ns = monotonic_ns();
    g_state.table[idx].applied_at_ns = g_state.table[idx].learned_at_ns;
    pthread_mutex_unlock(&g_state.lock);
    return sequence;
}

/* Caller must hold g_state.lock. True if `incoming` is newer than
   `existing` under (owner_epoch, sequence) ordering. */
static int is_newer(const struct daim_host_location *existing, const struct daim_host_location *incoming)
{
    if (incoming->owner_epoch != existing->owner_epoch) {
        return incoming->owner_epoch > existing->owner_epoch;
    }
    return incoming->sequence > existing->sequence;
}

static enum daim_host_apply_result apply_locked(const struct daim_host_location *update)
{
    long idx;
    enum daim_host_apply_result result;

    if (!update || update->origin_node_id == 0) {
        g_state.stats.applied_invalid++;
        return DAIM_HOST_APPLY_INVALID;
    }
    /* A node never accepts a remote claim to its own locally learned MAC;
       that is this node's own authoritative state. */
    if (update->origin_node_id == g_state.node_id) {
        g_state.stats.applied_invalid++;
        return DAIM_HOST_APPLY_INVALID;
    }

    idx = find_index(update->mac);
    if (idx < 0) {
        if (g_state.count >= MAX_HOST_ENTRIES) {
            g_state.stats.applied_invalid++;
            return DAIM_HOST_APPLY_INVALID;
        }
        idx = (long)g_state.count++;
        memset(&g_state.table[idx], 0, sizeof(g_state.table[idx]));
        memcpy(g_state.table[idx].mac, update->mac, MAC_ADDR_LEN);
        g_state.table[idx].origin_node_id = update->origin_node_id;
        g_state.table[idx].owner_dpid = update->owner_dpid;
        g_state.table[idx].owner_port = update->owner_port;
        g_state.table[idx].owner_epoch = update->owner_epoch;
        g_state.table[idx].sequence = update->sequence;
        g_state.table[idx].is_local = 0;
        g_state.table[idx].learned_at_ns = update->learned_at_ns;
        g_state.table[idx].applied_at_ns = monotonic_ns();
        g_state.stats.applied_new++;
        return DAIM_HOST_APPLY_NEW;
    }

    if (g_state.table[idx].origin_node_id != update->origin_node_id) {
        g_state.stats.applied_conflict++;
        return DAIM_HOST_APPLY_OWNERSHIP_CONFLICT;
    }

    if (update->owner_epoch == g_state.table[idx].owner_epoch &&
        update->sequence == g_state.table[idx].sequence) {
        g_state.stats.applied_duplicate++;
        return DAIM_HOST_APPLY_DUPLICATE;
    }

    if (!is_newer(&g_state.table[idx], update)) {
        g_state.stats.applied_stale_rejected++;
        return DAIM_HOST_APPLY_STALE_REJECTED;
    }

    g_state.table[idx].owner_dpid = update->owner_dpid;
    g_state.table[idx].owner_port = update->owner_port;
    g_state.table[idx].owner_epoch = update->owner_epoch;
    g_state.table[idx].sequence = update->sequence;
    g_state.table[idx].learned_at_ns = update->learned_at_ns;
    g_state.table[idx].applied_at_ns = monotonic_ns();
    g_state.stats.applied_updated++;
    result = DAIM_HOST_APPLY_UPDATED;
    return result;
}

enum daim_host_apply_result daim_host_apply_remote(const struct daim_host_location *update)
{
    enum daim_host_apply_result result;
    if (!g_state.ready) {
        return DAIM_HOST_APPLY_INVALID;
    }
    pthread_mutex_lock(&g_state.lock);
    result = apply_locked(update);
    pthread_mutex_unlock(&g_state.lock);
    return result;
}

enum daim_host_apply_result daim_host_import_snapshot_entry(const struct daim_host_location *entry)
{
    enum daim_host_apply_result result;
    if (!g_state.ready) {
        return DAIM_HOST_APPLY_INVALID;
    }
    pthread_mutex_lock(&g_state.lock);
    result = apply_locked(entry);
    pthread_mutex_unlock(&g_state.lock);
    return result;
}

int daim_host_lookup(const uint8_t mac[MAC_ADDR_LEN], struct daim_host_location *out)
{
    long idx;
    if (!g_state.ready || !mac) {
        return 0;
    }
    pthread_mutex_lock(&g_state.lock);
    idx = find_index(mac);
    if (idx < 0) {
        pthread_mutex_unlock(&g_state.lock);
        return 0;
    }
    if (out) {
        *out = g_state.table[idx];
    }
    pthread_mutex_unlock(&g_state.lock);
    return 1;
}

int daim_host_export_snapshot(daim_host_snapshot_emit_fn emit, void *context)
{
    size_t i, count;
    struct daim_host_location copy[MAX_HOST_ENTRIES];
    if (!g_state.ready || !emit) {
        return -1;
    }
    pthread_mutex_lock(&g_state.lock);
    count = g_state.count;
    memcpy(copy, g_state.table, count * sizeof(copy[0]));
    pthread_mutex_unlock(&g_state.lock);
    for (i = 0; i < count; ++i) {
        emit(context, &copy[i]);
    }
    return (int)count;
}

void daim_host_state_stats(struct daim_host_state_stats *out)
{
    if (!out) {
        return;
    }
    if (!g_state.ready) {
        memset(out, 0, sizeof(*out));
        return;
    }
    pthread_mutex_lock(&g_state.lock);
    *out = g_state.stats;
    out->local_count = 0;
    out->remote_count = 0;
    {
        size_t i;
        for (i = 0; i < g_state.count; ++i) {
            if (g_state.table[i].is_local) {
                out->local_count++;
            } else {
                out->remote_count++;
            }
        }
    }
    pthread_mutex_unlock(&g_state.lock);
}

void daim_host_state_reset(void)
{
    if (!g_state.ready) {
        return;
    }
    pthread_mutex_lock(&g_state.lock);
    g_state.count = 0;
    g_state.next_local_sequence = 1;
    memset(&g_state.stats, 0, sizeof(g_state.stats));
    pthread_mutex_unlock(&g_state.lock);
}
