#ifndef DAIM_DISTRIBUTED_LEARNING_APP_H
#define DAIM_DISTRIBUTED_LEARNING_APP_H

#include "daim_os_api.h"
#include "daim_switch_adapter.h"
#include "daim_peer_transport.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Distributed counterpart of daim_learning_app.c (that file is Paper 1's
   artifact and is intentionally left unmodified). Same NO_RULE-driven
   shape -- registers with daim_signal, still calls daim_table_write and
   the bound switch adapter's flow_add -- but the destination lookup
   consults daim_distributed_state's HOST_LOCATION table (local + remote
   cache) instead of a single process-local MAC table, and a learned local
   source is disseminated to peers via the given transport.

   Next-hop routing for this milestone is intentionally minimal: nodes are
   given a linear chain order (matching the h1-s1-s2-s3-s4-h4 gate
   topology), and a remote destination is routed toward whichever fixed
   port faces the neighbour in the direction of that destination's owning
   node in the chain. This is not a topology/routing protocol -- diamond
   or arbitrary topologies are out of scope for this milestone (see the
   user's own staged plan: linear first, diamond later). */

#define DAIM_DIST_MAX_CHAIN 64

/* chain_order lists every node's ID in a fixed line, e.g. {1,2,3,4} for
   h1-s1(node1)-s2(node2)-s3(node3)-s4(node4)-h4. my_node_id must appear in
   it. A destination owned by a node earlier in the array is routed out
   port_toward_lower; later, port_toward_higher.

   host_port is the switch port this node's own host is attached to (port 1
   in every switch of the gate topology). A source MAC is only ever local-
   learned (daim_host_learn_local, i.e. this node claiming ownership of it)
   when it arrives on host_port; a NO_RULE event for the same source MAC
   arriving on any other port is transit/flooded traffic passing through
   this switch, not a host attached to it, and must not be claimed. Without
   this check, a broadcast (e.g. ARP) from a host several hops away floods
   through every switch in the chain, and every switch it transits would
   incorrectly call daim_host_learn_local for that MAC too -- each with a
   different origin_node_id -- producing a cascade of spurious
   OWNERSHIP_CONFLICT results when those false claims are disseminated back
   to the MAC's real owner and to each other. Confirmed directly: this was
   the actual cause of G5/G6 failures in the first full gate run after
   G1-G4 started passing. */
int daim_distributed_learning_app_init(struct daim_switch_adapter *adapter,
                                        uint64_t my_node_id, uint64_t my_dpid, uint16_t host_port,
                                        const uint64_t *chain_order, size_t chain_len,
                                        uint16_t port_toward_lower, uint16_t port_toward_higher,
                                        struct daim_peer_transport *transport);

uint16_t daim_distributed_learning_app_packet_in(const char *bridge, struct no_rule_packet_info *info);

struct daim_distributed_learning_app_stats {
    uint64_t no_rule_events;
    uint64_t flows_installed;
    uint64_t local_hosts_learned;
    uint64_t remote_updates_disseminated;
};
void daim_distributed_learning_app_stats(struct daim_distributed_learning_app_stats *out);

void daim_distributed_learning_app_reset(void);

#ifdef __cplusplus
}
#endif

#endif
