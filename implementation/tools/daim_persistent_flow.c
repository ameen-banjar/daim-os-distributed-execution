/* CLI wrapper around the persistent OpenFlow adapter, mirroring
   daim_ovs_flow.c's shape so a benchmark harness can time it the same way
   (subprocess wall-clock around one invocation): connect, install exactly
   one flow, exit. The bridge must already have this program's listen port
   set as a controller target before it is started, e.g.:

       ovs-vsctl set-controller BRIDGE tcp:127.0.0.1:PORT

   Usage: daim_persistent_flow BRIDGE LISTEN_PORT FLOW [TIMEOUT_SECONDS] */
#include "ovs_persistent_adapter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc < 4 || argc > 5) {
        fprintf(stderr, "Usage: %s BRIDGE LISTEN_PORT FLOW [TIMEOUT_SECONDS]\n", argv[0]);
        return 2;
    }
    const char *bridge = argv[1];
    int port = atoi(argv[2]);
    const char *flow = argv[3];
    int timeout = argc == 5 ? atoi(argv[4]) : 10;
    (void)bridge; /* the persistent connection is already bound to one bridge */

    struct daim_switch_adapter adapter = {0};
    if (daim_ovs_persistent_adapter_create(&adapter, port, timeout) != 0) {
        fputs("Could not establish persistent OpenFlow connection\n", stderr);
        return 3;
    }
    int result = adapter.ops->flow_add(&adapter, bridge, flow);
    adapter.ops->destroy(&adapter);
    if (result != 0) {
        fprintf(stderr, "Flow-Mod send failed\n");
        return 4;
    }
    return 0;
}
