#include "daim_learning_app.h"
#include "daim_core.h"
#include "daim_switch_adapter.h"

#include <assert.h>
#include <string.h>

int main(void)
{
    struct daim_switch_adapter adapter = {0};
    struct daim_mock_adapter_stats stats;
    struct no_rule_packet_info info;
    uint64_t no_rule_events, flows_installed;
    size_t table_count;
    uint8_t host_a[6] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
    uint8_t host_b[6] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x02};

    assert(daim_init() == DAIM_CORE_OK);
    assert(daim_mock_adapter_create(&adapter) == 0);
    assert(daim_learning_app_init(&adapter) == 0);

    /* A speaks first on port 1; B is unknown, so the decision is flood. */
    memset(&info, 0, sizeof(info));
    info.in_port = 1;
    memcpy(info.mac_src, host_a, 6);
    memcpy(info.mac_dst, host_b, 6);
    assert(daim_learning_app_packet_in("s1", &info) == PORT_FLOOD);

    daim_learning_app_stats(&no_rule_events, &flows_installed, &table_count);
    assert(no_rule_events == 1);
    assert(flows_installed == 0);
    assert(table_count == 1);

    /* The flood decision above still ran the full pipeline: entry/decision/
       table-write/install/exit must be populated and monotonic, with
       installed == 0 since the destination was unknown. */
    {
        struct daim_learning_app_timing t;
        daim_learning_app_last_timing(&t);
        assert(t.entry_ns > 0);
        assert(t.decision_done_ns >= t.entry_ns);
        assert(t.table_write_done_ns >= t.decision_done_ns);
        assert(t.install_done_ns >= t.table_write_done_ns);
        assert(t.exit_ns >= t.install_done_ns);
        assert(t.installed == 0);
    }

    /* B replies on port 2; A is now known on port 1, so a flow is installed. */
    memset(&info, 0, sizeof(info));
    info.in_port = 2;
    memcpy(info.mac_src, host_b, 6);
    memcpy(info.mac_dst, host_a, 6);
    assert(daim_learning_app_packet_in("s1", &info) == 1);

    daim_learning_app_stats(&no_rule_events, &flows_installed, &table_count);
    assert(no_rule_events == 2);
    assert(flows_installed == 1);
    assert(table_count == 2);

    assert(daim_mock_adapter_get_stats(&adapter, &stats) == 0);
    assert(stats.flows_added == 1);
    assert(strcmp(stats.last_bridge, "s1") == 0);

    /* This second call installed a flow: installed == 1 and timestamps are
       still monotonic. */
    {
        struct daim_learning_app_timing t;
        daim_learning_app_last_timing(&t);
        assert(t.installed == 1);
        assert(t.decision_done_ns >= t.entry_ns);
        assert(t.table_write_done_ns >= t.decision_done_ns);
        assert(t.install_done_ns >= t.table_write_done_ns);
        assert(t.exit_ns >= t.install_done_ns);
    }

    /* A second bridge keeps an independent MAC table (unknown again). */
    memset(&info, 0, sizeof(info));
    info.in_port = 5;
    memcpy(info.mac_src, host_a, 6);
    memcpy(info.mac_dst, host_b, 6);
    assert(daim_learning_app_packet_in("s2", &info) == PORT_FLOOD);

    assert(daim_core_table_count(DAIM_PACKET_FORWARDING_TABLE) == 3);

    adapter.ops->destroy(&adapter);
    daim_quit();
    return 0;
}
