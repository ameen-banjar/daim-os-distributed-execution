#include "ovs_switch_adapter.h"

#include <stdio.h>
#include <string.h>

static void usage(const char *program)
{
    fprintf(stderr, "Usage: %s add|delete BRIDGE FLOW\n", program);
}

int main(int argc, char **argv)
{
    struct daim_switch_adapter adapter = {0};
    int result;
    if (argc != 4 || (strcmp(argv[1], "add") != 0 && strcmp(argv[1], "delete") != 0)) {
        usage(argv[0]);
        return 2;
    }
    if (daim_ovs_adapter_create(&adapter, daim_ovs_spawn_executor, NULL) != 0) {
        fputs("Could not create OVS adapter\n", stderr);
        return 3;
    }
    if (strcmp(argv[1], "add") == 0) {
        result = adapter.ops->flow_add(&adapter, argv[2], argv[3]);
    } else {
        result = adapter.ops->flow_delete(&adapter, argv[2], argv[3]);
    }
    adapter.ops->destroy(&adapter);
    if (result != 0) {
        fprintf(stderr, "OVS command failed with status %d\n", result);
        return 4;
    }
    return 0;
}

