#ifndef OVS_PERSISTENT_ADAPTER_H
#define OVS_PERSISTENT_ADAPTER_H

#include "daim_switch_adapter.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A southbound adapter that holds one persistent OpenFlow 1.3 TCP
   connection per bridge and sends raw OFPT_FLOW_MOD messages on it,
   instead of spawning an ovs-ofctl process per rule. The bridge must be
   configured with this adapter's listening port as an auxiliary controller
   target, e.g.:

       ovs-vsctl set-controller br0 tcp:127.0.0.1:6653 tcp:127.0.0.1:<port>

   so the switch connects to both the primary (Os-Ken) and this adapter.

   Supports exactly the flow-string subset used elsewhere in this codebase:
     "priority=P,in_port=N[,dl_dst=MAC],actions=output:M"
     "priority=P,ip,actions=normal"
   and, for flow_delete, a bare match subset of the same fields. */

/* Starts listening on listen_port and blocks until the switch connects and
   the OFPT_HELLO handshake completes, or timeout_seconds elapses.
   Returns 0 on success, -1 on failure (including timeout). */
int daim_ovs_persistent_adapter_create(struct daim_switch_adapter *adapter,
                                       int listen_port, int timeout_seconds);

/* Cumulative counters for evaluation. */
struct daim_persistent_adapter_stats {
    uint64_t flow_mods_sent;
    uint64_t bytes_sent;
    uint64_t echo_replies_sent;
};

int daim_ovs_persistent_adapter_get_stats(struct daim_switch_adapter *adapter,
                                          struct daim_persistent_adapter_stats *stats);

/* Exact on-wire OFPT_FLOW_MOD byte length for the given flow string, using
   the same OpenFlow 1.3 encoder as persistent_flow_add/persistent_flow_delete
   (build_match/build_actions/build_flow_mod). Pure computation, no socket
   involved: usable to size a Flow-Mod for any southbound path (including
   process-per-rule's ovs-ofctl invocations, whose wire message this
   adapter never itself sends) since the OpenFlow 1.3 message layout for a
   given match/action shape does not depend on which channel carries it.
   Returns 0 on a malformed flow string. */
size_t daim_ovs_wire_flow_mod_size(const char *flow, int is_delete);

#ifdef __cplusplus
}
#endif

#endif
