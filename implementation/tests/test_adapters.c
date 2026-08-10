#include "daim_switch_adapter.h"
#include "ovs_switch_adapter.h"

#include <assert.h>
#include <string.h>

struct capture { int calls; char command[32]; char bridge[64]; char flow[512]; };
static int capture_exec(void *context, const char *const argv[])
{
    struct capture *c = context; c->calls++;
    assert(strcmp(argv[1], "-O") == 0);
    assert(strcmp(argv[2], "OpenFlow13") == 0);
    strcpy(c->command, argv[3]); strcpy(c->bridge, argv[4]); strcpy(c->flow, argv[5]);
    assert(argv[6] == NULL); return 0;
}

int main(void)
{
    struct daim_switch_adapter adapter = {0};
    struct daim_mock_adapter_stats stats;
    uint8_t mac[6] = {0}; char input[] = "packet"; char output[16] = {0};
    struct capture capture = {0};
    assert(daim_mock_adapter_create(&adapter) == 0);
    assert(adapter.ops->port_write(&adapter, mac, input, sizeof(input)) == (int)sizeof(input));
    assert(adapter.ops->port_read(&adapter, mac, output, sizeof(output)) == (int)sizeof(input));
    assert(memcmp(input, output, sizeof(input)) == 0);
    assert(adapter.ops->flow_add(&adapter,"br-daim","priority=100,in_port=1,actions=output:2") == 0);
    assert(adapter.ops->flow_delete(&adapter,"br-daim","in_port=1") == 0);
    assert(daim_mock_adapter_get_stats(&adapter,&stats) == 0);
    assert(stats.writes == 1 && stats.reads == 1 && stats.flows_added == 1 && stats.flows_deleted == 1);
    adapter.ops->destroy(&adapter);

    assert(daim_ovs_adapter_create(&adapter,capture_exec,&capture) == 0);
    assert(adapter.ops->flow_add(&adapter,"br-daim","priority=100,ip,actions=drop") == 0);
    assert(capture.calls == 1 && strcmp(capture.command,"add-flow") == 0 && strcmp(capture.bridge,"br-daim") == 0);
    assert(adapter.ops->flow_delete(&adapter,"br-daim","ip") == 0);
    assert(capture.calls == 2 && strcmp(capture.command,"del-flows") == 0);
    assert(adapter.ops->flow_add(&adapter,"bad\nbridge","actions=drop") == -1);
    assert(adapter.ops->port_read(&adapter,mac,output,sizeof(output)) == -1);
    adapter.ops->destroy(&adapter);
    return 0;
}
