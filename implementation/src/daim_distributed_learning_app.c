#include "daim_distributed_learning_app.h"
#include "daim_distributed_state.h"
#include "daim_core.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>

#define MAX_BRIDGE_LEN 64

struct app_state {
    pthread_mutex_t lock;
    int ready;
    struct daim_switch_adapter *adapter;
    struct daim_peer_transport *transport;
    uint64_t my_node_id;
    uint64_t my_dpid;
    uint16_t host_port;
    uint64_t chain_order[DAIM_DIST_MAX_CHAIN];
    size_t chain_len;
    uint16_t port_toward_lower;
    uint16_t port_toward_higher;
    char current_bridge[MAX_BRIDGE_LEN];
    uint16_t decided_port;
    struct daim_distributed_learning_app_stats stats;
};

static struct app_state g_app;

static long chain_index_of(uint64_t node_id)
{
    size_t i;
    for (i = 0; i < g_app.chain_len; ++i) {
        if (g_app.chain_order[i] == node_id) {
            return (long)i;
        }
    }
    return -1;
}

/* Returns PORT_NONE if the target node is not in this node's configured
   chain -- a real gap (unknown topology position), reported distinctly
   from PORT_FLOOD (destination MAC simply not learned yet anywhere). */
static uint16_t next_hop_port(uint64_t target_node_id)
{
    long my_idx = chain_index_of(g_app.my_node_id);
    long target_idx = chain_index_of(target_node_id);
    if (my_idx < 0 || target_idx < 0) {
        return PORT_NONE;
    }
    return target_idx < my_idx ? g_app.port_toward_lower : g_app.port_toward_higher;
}

static void format_mac(char *out, size_t outlen, const uint8_t mac[MAC_ADDR_LEN])
{
    snprintf(out, outlen, "%02x:%02x:%02x:%02x:%02x:%02x",
              mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

static void install_flow(const char *bridge, uint16_t in_port, const uint8_t mac_dst[MAC_ADDR_LEN], uint16_t out_port)
{
    char macbuf[18];
    char flow[256];
    if (!g_app.adapter || !g_app.adapter->ops || !g_app.adapter->ops->flow_add) {
        return;
    }
    format_mac(macbuf, sizeof(macbuf), mac_dst);
    snprintf(flow, sizeof(flow), "priority=100,in_port=%u,dl_dst=%s,actions=output:%u",
             (unsigned)in_port, macbuf, (unsigned)out_port);
    if (g_app.adapter->ops->flow_add(g_app.adapter, bridge, flow) == 0) {
        g_app.stats.flows_installed++;
    }
}

/* Registered with daim_signal(NO_RULE, ...); invoked from
   daim_distributed_learning_app_packet_in, which holds g_app.lock for the
   whole call -- this must not take the lock itself. */
static void on_no_rule(uint16_t sig_type, void *data)
{
    struct no_rule_packet_info *info = data;
    struct packet_forwarding_table_entry entry;
    struct packet_action_output action;
    uint8_t buf[sizeof(struct packet_forwarding_table_entry) + sizeof(struct packet_action_output)];
    struct daim_host_location dest_loc;
    uint16_t out_port;
    uint64_t sequence;
    (void)sig_type;
    if (!info) {
        return;
    }

    g_app.stats.no_rule_events++;

    /* This node is authoritative only for what actually arrives on its own
       host port -- NOT for every source MAC it happens to see a NO_RULE
       for, since a flooded/broadcast packet from a host several hops away
       (e.g. an ARP request while the destination is still unknown) reaches
       this switch too, on an inter-switch port, and is transit traffic,
       not a local host. Claiming it here would falsely assert ownership
       and disseminate that false claim to the MAC's real owner and every
       other node, producing a cascade of spurious OWNERSHIP_CONFLICT
       results (confirmed as the actual root cause of an earlier gate
       failure). */
    if (info->in_port == g_app.host_port) {
        sequence = daim_host_learn_local(info->mac_src, g_app.my_dpid, info->in_port);
        if (sequence != 0) {
            struct daim_host_location learned;
            g_app.stats.local_hosts_learned++;
            if (daim_host_lookup(info->mac_src, &learned) && g_app.transport) {
                daim_peer_transport_disseminate(g_app.transport, &learned);
                g_app.stats.remote_updates_disseminated++;
            }
        }
    }

    if (daim_host_lookup(info->mac_dst, &dest_loc)) {
        out_port = dest_loc.is_local ? (uint16_t)dest_loc.owner_port : next_hop_port(dest_loc.origin_node_id);
    } else {
        out_port = PORT_FLOOD;
    }

    memset(&entry, 0, sizeof(entry));
    entry.in_port = info->in_port;
    memcpy(entry.mac_src, info->mac_src, MAC_ADDR_LEN);
    memcpy(entry.mac_dst, info->mac_dst, MAC_ADDR_LEN);
    entry.ethernet_type = info->ethernet_type;
    entry.weight = 100;
    entry.num_of_actions = 1;

    memset(buf, 0, sizeof(buf));
    memcpy(buf, &entry, sizeof(entry));
    memset(&action, 0, sizeof(action));
    action.type = PACKET_OUTPUT;
    action.port = out_port;
    memcpy(buf + sizeof(entry), &action, sizeof(action));

    daim_table_write(DAIM_PACKET_FORWARDING_TABLE, buf, (uint32_t)sizeof(buf), ADD);

    if (out_port != PORT_FLOOD && out_port != PORT_NONE) {
        install_flow(g_app.current_bridge, info->in_port, info->mac_dst, out_port);
    }

    g_app.decided_port = out_port;
}

int daim_distributed_learning_app_init(struct daim_switch_adapter *adapter,
                                        uint64_t my_node_id, uint64_t my_dpid, uint16_t host_port,
                                        const uint64_t *chain_order, size_t chain_len,
                                        uint16_t port_toward_lower, uint16_t port_toward_higher,
                                        struct daim_peer_transport *transport)
{
    if (!chain_order || chain_len == 0 || chain_len > DAIM_DIST_MAX_CHAIN) {
        return -1;
    }
    if (!g_app.ready) {
        if (pthread_mutex_init(&g_app.lock, NULL) != 0) {
            return -1;
        }
        g_app.ready = 1;
    }
    pthread_mutex_lock(&g_app.lock);
    g_app.adapter = adapter;
    g_app.transport = transport;
    g_app.my_node_id = my_node_id;
    g_app.my_dpid = my_dpid;
    g_app.host_port = host_port;
    memcpy(g_app.chain_order, chain_order, chain_len * sizeof(chain_order[0]));
    g_app.chain_len = chain_len;
    g_app.port_toward_lower = port_toward_lower;
    g_app.port_toward_higher = port_toward_higher;
    memset(g_app.current_bridge, 0, sizeof(g_app.current_bridge));
    memset(&g_app.stats, 0, sizeof(g_app.stats));
    pthread_mutex_unlock(&g_app.lock);
    daim_signal(NO_RULE, on_no_rule);
    return 0;
}

uint16_t daim_distributed_learning_app_packet_in(const char *bridge, struct no_rule_packet_info *info)
{
    uint16_t result;
    if (!g_app.ready || !bridge || !info) {
        return PORT_NONE;
    }
    pthread_mutex_lock(&g_app.lock);
    snprintf(g_app.current_bridge, MAX_BRIDGE_LEN, "%s", bridge);
    daim_core_emit(NO_RULE, info);
    result = g_app.decided_port;
    pthread_mutex_unlock(&g_app.lock);
    return result;
}

void daim_distributed_learning_app_stats(struct daim_distributed_learning_app_stats *out)
{
    if (!out) {
        return;
    }
    if (!g_app.ready) {
        memset(out, 0, sizeof(*out));
        return;
    }
    pthread_mutex_lock(&g_app.lock);
    *out = g_app.stats;
    pthread_mutex_unlock(&g_app.lock);
}

void daim_distributed_learning_app_reset(void)
{
    if (!g_app.ready) {
        return;
    }
    pthread_mutex_lock(&g_app.lock);
    memset(&g_app.stats, 0, sizeof(g_app.stats));
    pthread_mutex_unlock(&g_app.lock);
}
