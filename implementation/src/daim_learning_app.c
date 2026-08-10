#define _POSIX_C_SOURCE 200809L

#include "daim_learning_app.h"
#include "daim_core.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define MAX_ENTRIES 4096
#define MAX_BRIDGE_LEN 64

struct mac_entry {
    char bridge[MAX_BRIDGE_LEN];
    uint8_t mac[MAC_ADDR_LEN];
    uint16_t port;
};

struct app_state {
    pthread_mutex_t lock;
    int ready;
    struct daim_switch_adapter *adapter;
    struct mac_entry table[MAX_ENTRIES];
    size_t table_count;
    char current_bridge[MAX_BRIDGE_LEN];
    uint16_t decided_port;
    uint64_t no_rule_events;
    uint64_t flows_installed;
    struct daim_learning_app_timing last_timing;
};

static struct app_state g_app;

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint16_t lookup_port(const char *bridge, const uint8_t mac[MAC_ADDR_LEN])
{
    size_t i;
    for (i = 0; i < g_app.table_count; ++i) {
        if (strcmp(g_app.table[i].bridge, bridge) == 0 &&
            memcmp(g_app.table[i].mac, mac, MAC_ADDR_LEN) == 0) {
            return g_app.table[i].port;
        }
    }
    return PORT_FLOOD;
}

static void learn(const char *bridge, const uint8_t mac[MAC_ADDR_LEN], uint16_t port)
{
    size_t i;
    struct mac_entry *e;
    for (i = 0; i < g_app.table_count; ++i) {
        if (strcmp(g_app.table[i].bridge, bridge) == 0 &&
            memcmp(g_app.table[i].mac, mac, MAC_ADDR_LEN) == 0) {
            g_app.table[i].port = port;
            return;
        }
    }
    if (g_app.table_count >= MAX_ENTRIES) {
        return;
    }
    e = &g_app.table[g_app.table_count++];
    memset(e, 0, sizeof(*e));
    snprintf(e->bridge, MAX_BRIDGE_LEN, "%s", bridge);
    memcpy(e->mac, mac, MAC_ADDR_LEN);
    e->port = port;
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
        g_app.flows_installed++;
    }
}

/* Registered with daim_signal(NO_RULE, ...). Only ever invoked from within
   daim_learning_app_packet_in, which already holds g_app.lock for the whole
   call, so this must not take the lock itself. */
static void on_no_rule(uint16_t sig_type, void *data)
{
    struct no_rule_packet_info *info = data;
    struct packet_forwarding_table_entry entry;
    struct packet_action_output action;
    uint8_t buf[sizeof(struct packet_forwarding_table_entry) + sizeof(struct packet_action_output)];
    uint16_t out_port;
    (void)sig_type;
    if (!info) {
        return;
    }

    g_app.no_rule_events++;

    learn(g_app.current_bridge, info->mac_src, info->in_port);
    out_port = lookup_port(g_app.current_bridge, info->mac_dst);
    g_app.last_timing.decision_done_ns = monotonic_ns();

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
    g_app.last_timing.table_write_done_ns = monotonic_ns();

    g_app.last_timing.installed = 0;
    if (out_port != PORT_FLOOD) {
        install_flow(g_app.current_bridge, info->in_port, info->mac_dst, out_port);
        g_app.last_timing.installed = 1;
    }
    g_app.last_timing.install_done_ns = monotonic_ns();

    g_app.decided_port = out_port;
}

int daim_learning_app_init(struct daim_switch_adapter *adapter)
{
    if (!g_app.ready) {
        if (pthread_mutex_init(&g_app.lock, NULL) != 0) {
            return -1;
        }
        g_app.ready = 1;
    }
    pthread_mutex_lock(&g_app.lock);
    g_app.adapter = adapter;
    g_app.table_count = 0;
    g_app.no_rule_events = 0;
    g_app.flows_installed = 0;
    memset(g_app.current_bridge, 0, sizeof(g_app.current_bridge));
    memset(&g_app.last_timing, 0, sizeof(g_app.last_timing));
    pthread_mutex_unlock(&g_app.lock);
    daim_signal(NO_RULE, on_no_rule);
    return 0;
}

uint16_t daim_learning_app_packet_in(const char *bridge, struct no_rule_packet_info *info)
{
    uint16_t result;
    if (!g_app.ready || !bridge || !info) {
        return PORT_NONE;
    }
    pthread_mutex_lock(&g_app.lock);
    g_app.last_timing.entry_ns = monotonic_ns();
    snprintf(g_app.current_bridge, MAX_BRIDGE_LEN, "%s", bridge);
    daim_core_emit(NO_RULE, info);
    result = g_app.decided_port;
    g_app.last_timing.exit_ns = monotonic_ns();
    pthread_mutex_unlock(&g_app.lock);
    return result;
}

void daim_learning_app_last_timing(struct daim_learning_app_timing *out)
{
    if (!out) {
        return;
    }
    if (!g_app.ready) {
        memset(out, 0, sizeof(*out));
        return;
    }
    pthread_mutex_lock(&g_app.lock);
    *out = g_app.last_timing;
    pthread_mutex_unlock(&g_app.lock);
}

void daim_learning_app_stats(uint64_t *no_rule_events, uint64_t *flows_installed, size_t *table_count)
{
    if (!g_app.ready) {
        if (no_rule_events) *no_rule_events = 0;
        if (flows_installed) *flows_installed = 0;
        if (table_count) *table_count = 0;
        return;
    }
    pthread_mutex_lock(&g_app.lock);
    if (no_rule_events) *no_rule_events = g_app.no_rule_events;
    if (flows_installed) *flows_installed = g_app.flows_installed;
    if (table_count) *table_count = g_app.table_count;
    pthread_mutex_unlock(&g_app.lock);
}

void daim_learning_app_reset(void)
{
    if (!g_app.ready) {
        return;
    }
    pthread_mutex_lock(&g_app.lock);
    g_app.table_count = 0;
    g_app.no_rule_events = 0;
    g_app.flows_installed = 0;
    memset(g_app.current_bridge, 0, sizeof(g_app.current_bridge));
    pthread_mutex_unlock(&g_app.lock);
}
