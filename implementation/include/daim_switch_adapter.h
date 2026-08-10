#ifndef DAIM_SWITCH_ADAPTER_H
#define DAIM_SWITCH_ADAPTER_H

#include <stddef.h>
#include <stdint.h>

struct daim_switch_adapter;

struct daim_switch_adapter_ops {
    int (*port_read)(struct daim_switch_adapter *, const uint8_t mac[6], void *buffer, uint64_t size);
    int (*port_write)(struct daim_switch_adapter *, const uint8_t mac[6], const void *buffer, uint64_t size);
    int (*switch_ioctl)(struct daim_switch_adapter *, uint64_t request_code, void *data);
    int (*flow_add)(struct daim_switch_adapter *, const char *bridge, const char *flow);
    int (*flow_delete)(struct daim_switch_adapter *, const char *bridge, const char *flow_match);
    void (*destroy)(struct daim_switch_adapter *);
};

struct daim_switch_adapter {
    const struct daim_switch_adapter_ops *ops;
    void *context;
};

struct daim_mock_adapter_stats {
    uint64_t reads;
    uint64_t writes;
    uint64_t ioctls;
    uint64_t flows_added;
    uint64_t flows_deleted;
    char last_bridge[64];
    char last_flow[512];
};

int daim_mock_adapter_create(struct daim_switch_adapter *adapter);
int daim_mock_adapter_get_stats(struct daim_switch_adapter *adapter, struct daim_mock_adapter_stats *stats);

#endif

