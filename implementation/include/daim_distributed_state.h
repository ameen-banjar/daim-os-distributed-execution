#ifndef DAIM_DISTRIBUTED_STATE_H
#define DAIM_DISTRIBUTED_STATE_H

#include "daim_os_api.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* One node's view of where a MAC address is attached, disseminated between
   independently executing DAIM nodes. Ownership is versioned by
   (origin_node_id, owner_epoch, sequence): sequence increases within one
   node's owner_epoch, and owner_epoch orders across restarts of that node.
   This is a per-origin epoch/sequence ordering, not a vector clock -- it
   totally orders one origin's own updates and lets a receiver reject stale
   or duplicate ones from that origin, but it does not establish a causal
   order between different origins' updates, and it does not by itself
   resolve two origins concurrently claiming the same MAC (that is
   OWNERSHIP_CONFLICT, reported and left unresolved -- see the "not yet
   handled" note below).

   Fencing/durability assumption for this milestone: origin_node_id is an
   operator-assigned identifier (unique per deployment by configuration, not
   by a discovery protocol), and owner_epoch is supplied by the operator at
   process start (a fixed experimental value in the current gate) rather
   than self-managed durable storage. A wall-clock timestamp is not treated
   as a fencing guarantee. Nothing in
   this module prevents a node from being restarted with a reused or lower
   epoch and thereby losing fencing; that requires persistent epoch storage
   and is out of scope for the prototype gate. Do not describe this
   ordering as conflict-resolving or eventually consistent -- concurrent-
   ownership handling (E8: two origins claiming the same MAC), causal
   ordering between origins, and deletion/tombstone propagation are
   explicitly not implemented here. */
struct daim_host_location {
    uint8_t mac[MAC_ADDR_LEN];
    uint64_t origin_node_id;
    uint64_t owner_dpid;
    uint32_t owner_port;
    uint64_t owner_epoch;
    uint64_t sequence;
    uint8_t is_local;
    uint64_t learned_at_ns;
    uint64_t applied_at_ns;
};

enum daim_host_apply_result {
    DAIM_HOST_APPLY_NEW = 0,
    DAIM_HOST_APPLY_UPDATED,
    DAIM_HOST_APPLY_DUPLICATE,
    DAIM_HOST_APPLY_STALE_REJECTED,
    DAIM_HOST_APPLY_OWNERSHIP_CONFLICT,
    DAIM_HOST_APPLY_INVALID,
};

/* Initialises this node's own distributed-state table. node_id identifies
   this node in every record it originates. Independent of daim_init()/the
   Core forwarding table -- this is a separate table, not a replacement. */
int daim_host_state_init(uint64_t node_id, uint64_t owner_epoch);

/* Records this node's own authoritative observation of a locally attached
   host (is_local=1, origin_node_id=this node, sequence auto-incremented).
   Returns the assigned sequence number, or 0 on failure -- callers that
   also disseminate to peers should do so only after a successful call. */
uint64_t daim_host_learn_local(const uint8_t mac[MAC_ADDR_LEN], uint64_t dpid, uint32_t port);

/* Applies an update received from a peer (is_local=0). Enforces
   (origin_node_id, owner_epoch, sequence) ordering per MAC: a lower
   owner_epoch, or an equal-or-lower sequence within the same owner_epoch,
   is rejected rather than silently overwriting existing state. A record
   already owned by a different origin_node_id for the same MAC is reported
   as OWNERSHIP_CONFLICT and not applied. */
enum daim_host_apply_result daim_host_apply_remote(const struct daim_host_location *update);

/* Local-first, remote-cache-fallback lookup: returns 1 and fills *out if
   the MAC is known (local or remote), 0 if unknown to this node. Never
   blocks on peer I/O -- it only reads this node's own table. */
int daim_host_lookup(const uint8_t mac[MAC_ADDR_LEN], struct daim_host_location *out);

/* Snapshot walk for reconnect resynchronisation: calls emit(context, entry)
   for every record currently held (local and remote-cached). Returns the
   number of entries emitted, or -1 on error. */
typedef void (*daim_host_snapshot_emit_fn)(void *context, const struct daim_host_location *entry);
int daim_host_export_snapshot(daim_host_snapshot_emit_fn emit, void *context);

/* Applies one snapshot entry received from a peer during resync; same
   ordering rules as daim_host_apply_remote. */
enum daim_host_apply_result daim_host_import_snapshot_entry(const struct daim_host_location *entry);

struct daim_host_state_stats {
    uint64_t local_count;
    uint64_t remote_count;
    uint64_t applied_new;
    uint64_t applied_updated;
    uint64_t applied_duplicate;
    uint64_t applied_stale_rejected;
    uint64_t applied_conflict;
    uint64_t applied_invalid;
};
void daim_host_state_stats(struct daim_host_state_stats *out);

void daim_host_state_reset(void);

#ifdef __cplusplus
}
#endif

#endif
