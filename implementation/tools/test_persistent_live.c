/* Manual smoke-test driver: connects the persistent OpenFlow adapter to a
   real OVS bridge (which must already have this program's listen port set
   as an auxiliary controller target) and installs one flow.

   Usage: test_persistent_live <bridge> <listen_port>
   Verify with: ovs-ofctl -O OpenFlow13 dump-flows <bridge> */
#include "ovs_persistent_adapter.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <bridge> <listen_port>\n", argv[0]);
        return 2;
    }
    const char *bridge = argv[1];
    int port = atoi(argv[2]);

    struct daim_switch_adapter adapter = {0};
    fprintf(stderr, "waiting for %s to connect to 127.0.0.1:%d ...\n", bridge, port);
    if (daim_ovs_persistent_adapter_create(&adapter, port, 15) != 0) {
        fprintf(stderr, "FAILED: handshake did not complete\n");
        return 1;
    }
    fprintf(stderr, "connected and handshake complete\n");

    if (adapter.ops->flow_add(&adapter, bridge,
            "priority=100,in_port=1,dl_dst=00:11:22:33:44:55,actions=output:2") != 0) {
        fprintf(stderr, "FAILED: flow_add\n");
        return 1;
    }
    fprintf(stderr, "flow_add sent\n");

    if (adapter.ops->flow_add(&adapter, bridge, "priority=100,ip,actions=normal") != 0) {
        fprintf(stderr, "FAILED: flow_add (ip/normal)\n");
        return 1;
    }
    fprintf(stderr, "flow_add (ip/normal) sent\n");

    struct daim_persistent_adapter_stats stats;
    daim_ovs_persistent_adapter_get_stats(&adapter, &stats);
    fprintf(stderr, "flow_mods_sent=%llu bytes_sent=%llu\n",
            (unsigned long long)stats.flow_mods_sent,
            (unsigned long long)stats.bytes_sent);

    sleep(1); /* give OVS a moment before we close the connection */
    adapter.ops->destroy(&adapter);
    printf("OK\n");
    return 0;
}
