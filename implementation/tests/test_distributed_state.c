#include "daim_distributed_state.h"

#include <assert.h>
#include <string.h>

static uint8_t MAC_A[MAC_ADDR_LEN] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
static uint8_t MAC_B[MAC_ADDR_LEN] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x02};

static struct daim_host_location make_update(uint64_t origin, uint64_t epoch, uint64_t seq,
                                              const uint8_t mac[MAC_ADDR_LEN], uint32_t port)
{
    struct daim_host_location u;
    memset(&u, 0, sizeof(u));
    memcpy(u.mac, mac, MAC_ADDR_LEN);
    u.origin_node_id = origin;
    u.owner_dpid = 100 + origin;
    u.owner_port = port;
    u.owner_epoch = epoch;
    u.sequence = seq;
    u.is_local = 0;
    u.learned_at_ns = 1;
    return u;
}

static int emitted_count;
static void count_emit(void *ctx, const struct daim_host_location *entry)
{
    (void)ctx;
    (void)entry;
    emitted_count++;
}

int main(void)
{
    struct daim_host_location out;
    struct daim_host_state_stats stats;
    struct daim_host_location u;

    /* This node is node_id=1; remote updates always originate elsewhere. */
    assert(daim_host_state_init(1, 7) == 0);

    /* Local learn: sequence starts at 1 and increments. */
    assert(daim_host_learn_local(MAC_A, /*dpid=*/1, /*port=*/1) == 1);
    assert(daim_host_learn_local(MAC_A, 1, 2) == 2); /* re-learn same host, new port */
    assert(daim_host_lookup(MAC_A, &out) == 1);
    assert(out.is_local == 1);
    assert(out.owner_port == 2);
    assert(out.sequence == 2);

    /* Unknown MAC. */
    assert(daim_host_lookup(MAC_B, &out) == 0);

    /* Remote NEW. */
    u = make_update(4, 1, 50, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_NEW);
    assert(daim_host_lookup(MAC_B, &out) == 1);
    assert(out.is_local == 0);
    assert(out.sequence == 50);

    /* Remote UPDATED: higher sequence, same epoch. */
    u = make_update(4, 1, 51, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_UPDATED);

    /* Remote DUPLICATE: exact same (epoch, sequence) resent. */
    u = make_update(4, 1, 51, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_DUPLICATE);

    /* Remote STALE_REJECTED: lower sequence, same epoch. */
    u = make_update(4, 1, 48, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_STALE_REJECTED);

    /* Remote UPDATED again: sequence continues past the accepted 51. */
    u = make_update(4, 1, 52, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_UPDATED);

    /* Older epoch is stale even with a higher sequence number. */
    u = make_update(4, 0, 9999, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_STALE_REJECTED);

    /* Newer epoch wins even with sequence reset to 1 (node restart). */
    u = make_update(4, 2, 1, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_UPDATED);
    assert(daim_host_lookup(MAC_B, &out) == 1);
    assert(out.owner_epoch == 2 && out.sequence == 1);

    /* Ownership conflict: a different origin claims a MAC this node
       already has on record for origin 4. */
    u = make_update(5, 1, 1, MAC_B, 9);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_OWNERSHIP_CONFLICT);
    /* No silent overwrite: record still shows origin 4. */
    assert(daim_host_lookup(MAC_B, &out) == 1);
    assert(out.origin_node_id == 4);

    /* Invalid: a remote update claiming to originate from this node itself. */
    u = make_update(1, 1, 1, MAC_A, 3);
    assert(daim_host_apply_remote(&u) == DAIM_HOST_APPLY_INVALID);

    /* Snapshot export sees both the local and remote entries. */
    emitted_count = 0;
    assert(daim_host_export_snapshot(count_emit, NULL) == 2);
    assert(emitted_count == 2);

    /* Snapshot import applies the same ordering rules as apply_remote. */
    daim_host_state_reset();
    assert(daim_host_lookup(MAC_B, &out) == 0);
    u = make_update(4, 1, 10, MAC_B, 9);
    assert(daim_host_import_snapshot_entry(&u) == DAIM_HOST_APPLY_NEW);
    u = make_update(4, 1, 5, MAC_B, 9);
    assert(daim_host_import_snapshot_entry(&u) == DAIM_HOST_APPLY_STALE_REJECTED);

    /* Ownership conflict via snapshot import: a cached record from a
       different origin for the same MAC (G8 scenario -- two nodes'
       snapshots disagree on who owns a MAC) must be rejected outright,
       not silently merged into this node's table. */
    u = make_update(5, 1, 1, MAC_B, 9);
    assert(daim_host_import_snapshot_entry(&u) == DAIM_HOST_APPLY_OWNERSHIP_CONFLICT);
    assert(daim_host_lookup(MAC_B, &out) == 1);
    assert(out.origin_node_id == 4);

    /* A peer's cached snapshot must never recreate or overwrite this
       node's own authoritative origin. */
    u = make_update(1, 99, 999, MAC_A, 7);
    assert(daim_host_import_snapshot_entry(&u) == DAIM_HOST_APPLY_INVALID);

    daim_host_state_stats(&stats);
    assert(stats.applied_new == 1);
    assert(stats.applied_stale_rejected == 1);
    assert(stats.applied_conflict == 1);
    assert(stats.applied_invalid == 1);
    assert(stats.remote_count == 1);
    assert(stats.local_count == 0);

    return 0;
}
