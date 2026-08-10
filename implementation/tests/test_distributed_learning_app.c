#include "daim_distributed_learning_app.h"
#include "daim_distributed_state.h"
#include "daim_core.h"
#include "daim_switch_adapter.h"

#include <assert.h>
#include <string.h>

#define HOST_PORT 1
#define PORT_TOWARD_LOWER 2
#define PORT_TOWARD_HIGHER 3

static const uint64_t CHAIN[4] = {1, 2, 3, 4};
static uint8_t HOST_A[6] = {0, 0, 0, 0, 0, 0x01}; /* the one host directly attached to this node */
static uint8_t HOST_REMOTE[6] = {0, 0, 0, 0, 0, 0x09}; /* owned by node 3, injected directly */
static uint8_t TRANSIT_HOST[6] = {0, 0, 0, 0, 0, 0x42}; /* someone else's host, seen only in transit */

int main(void)
{
    struct daim_switch_adapter adapter = {0};
    struct daim_mock_adapter_stats stats;
    struct no_rule_packet_info info;
    struct daim_distributed_learning_app_stats app_stats;
    struct daim_host_location remote, out;

    assert(daim_init() == DAIM_CORE_OK);
    assert(daim_mock_adapter_create(&adapter) == 0);
    /* This node is node_id=1, dpid=101; no transport (NULL) -- dissemination
       is exercised end-to-end by test_peer_transport.c and the multi-
       process gate, not needed to test the local decision logic here. */
    assert(daim_host_state_init(1, /*owner_epoch=*/1) == 0);
    assert(daim_distributed_learning_app_init(&adapter, 1, 101, HOST_PORT, CHAIN, 4,
                                               PORT_TOWARD_LOWER, PORT_TOWARD_HIGHER, NULL) == 0);

    /* A speaks first on the host port; its destination is unknown
       anywhere, so flood. */
    memset(&info, 0, sizeof(info));
    info.in_port = HOST_PORT;
    memcpy(info.mac_src, HOST_A, 6);
    memcpy(info.mac_dst, HOST_REMOTE, 6);
    assert(daim_distributed_learning_app_packet_in("s1", &info) == PORT_FLOOD);

    daim_distributed_learning_app_stats(&app_stats);
    assert(app_stats.no_rule_events == 1);
    assert(app_stats.local_hosts_learned == 1);
    assert(app_stats.flows_installed == 0);

    /* Regression test for the exact bug found in the Milestone 1 gate run:
       a NO_RULE event for some other host's MAC arriving on a non-host
       (inter-switch) port is transit/flooded traffic, not a host attached
       to this switch, and must NOT be locally learned -- doing so
       previously caused every switch a broadcast passed through to
       falsely claim ownership of the source MAC, producing a cascade of
       OWNERSHIP_CONFLICT results once disseminated back to the MAC's real
       owner. */
    memset(&info, 0, sizeof(info));
    info.in_port = PORT_TOWARD_LOWER;
    memcpy(info.mac_src, TRANSIT_HOST, 6);
    memcpy(info.mac_dst, HOST_A, 6);
    assert(daim_distributed_learning_app_packet_in("s1", &info) == HOST_PORT); /* dest A is local */

    daim_distributed_learning_app_stats(&app_stats);
    assert(app_stats.no_rule_events == 2);
    assert(app_stats.local_hosts_learned == 1); /* unchanged: TRANSIT_HOST was not learned */
    assert(daim_host_lookup(TRANSIT_HOST, &out) == 0); /* still unknown to this node */

    /* That said transit packet's destination (A) was already known and
       local, so it still installed a direct local-delivery flow. */
    daim_distributed_learning_app_stats(&app_stats);
    assert(app_stats.flows_installed == 1);
    assert(daim_mock_adapter_get_stats(&adapter, &stats) == 0);
    assert(stats.flows_added == 1);
    assert(strstr(stats.last_flow, "actions=output:1") != NULL);

    /* A remote host, as if learned via peer dissemination from node 3
       (chain index 2, later than this node's index 0): routing must use
       port_toward_higher, not the local owner_port from that record. */
    memset(&remote, 0, sizeof(remote));
    memcpy(remote.mac, HOST_REMOTE, 6);
    remote.origin_node_id = 3;
    remote.owner_dpid = 103;
    remote.owner_port = 7; /* node 3's local port -- must NOT appear on s1's Flow-Mod */
    remote.owner_epoch = 1;
    remote.sequence = 1;
    remote.is_local = 0;
    assert(daim_host_apply_remote(&remote) == DAIM_HOST_APPLY_NEW);

    memset(&info, 0, sizeof(info));
    info.in_port = HOST_PORT;
    memcpy(info.mac_src, HOST_A, 6);
    memcpy(info.mac_dst, HOST_REMOTE, 6);
    assert(daim_distributed_learning_app_packet_in("s1", &info) == PORT_TOWARD_HIGHER);

    assert(daim_mock_adapter_get_stats(&adapter, &stats) == 0);
    assert(strstr(stats.last_flow, "actions=output:3") != NULL);
    assert(strstr(stats.last_flow, "actions=output:7") == NULL);

    adapter.ops->destroy(&adapter);
    daim_quit();
    return 0;
}
