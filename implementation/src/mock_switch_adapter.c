#include "daim_switch_adapter.h"

#include <stdlib.h>
#include <string.h>

struct mock_context {
    struct daim_mock_adapter_stats stats;
    uint8_t last_packet[2048];
    size_t last_packet_size;
};

static int mock_read(struct daim_switch_adapter *adapter, const uint8_t mac[6], void *buffer, uint64_t size)
{
    struct mock_context *ctx = adapter->context;
    size_t amount;
    (void)mac;
    if (!buffer) return -1;
    amount = size < ctx->last_packet_size ? (size_t)size : ctx->last_packet_size;
    memcpy(buffer, ctx->last_packet, amount);
    ctx->stats.reads++;
    return (int)amount;
}

static int mock_write(struct daim_switch_adapter *adapter, const uint8_t mac[6], const void *buffer, uint64_t size)
{
    struct mock_context *ctx = adapter->context;
    (void)mac;
    if (!buffer || size > sizeof(ctx->last_packet)) return -1;
    memcpy(ctx->last_packet, buffer, (size_t)size);
    ctx->last_packet_size = (size_t)size;
    ctx->stats.writes++;
    return (int)size;
}

static int mock_ioctl(struct daim_switch_adapter *adapter, uint64_t request_code, void *data)
{
    struct mock_context *ctx = adapter->context;
    (void)request_code; (void)data;
    ctx->stats.ioctls++;
    return 0;
}

static int record_flow(struct daim_switch_adapter *adapter, const char *bridge, const char *flow, int add)
{
    struct mock_context *ctx = adapter->context;
    if (!bridge || !flow || strlen(bridge) >= sizeof(ctx->stats.last_bridge) || strlen(flow) >= sizeof(ctx->stats.last_flow)) return -1;
    strcpy(ctx->stats.last_bridge, bridge);
    strcpy(ctx->stats.last_flow, flow);
    if (add) ctx->stats.flows_added++; else ctx->stats.flows_deleted++;
    return 0;
}

static int mock_flow_add(struct daim_switch_adapter *a, const char *b, const char *f) { return record_flow(a,b,f,1); }
static int mock_flow_delete(struct daim_switch_adapter *a, const char *b, const char *f) { return record_flow(a,b,f,0); }

static void mock_destroy(struct daim_switch_adapter *adapter)
{
    free(adapter->context); adapter->context = NULL; adapter->ops = NULL;
}

static const struct daim_switch_adapter_ops ops = {
    mock_read, mock_write, mock_ioctl, mock_flow_add, mock_flow_delete, mock_destroy
};

int daim_mock_adapter_create(struct daim_switch_adapter *adapter)
{
    if (!adapter) return -1;
    adapter->context = calloc(1, sizeof(struct mock_context));
    if (!adapter->context) return -1;
    adapter->ops = &ops;
    return 0;
}

int daim_mock_adapter_get_stats(struct daim_switch_adapter *adapter, struct daim_mock_adapter_stats *stats)
{
    if (!adapter || !adapter->context || !stats) return -1;
    *stats = ((struct mock_context *)adapter->context)->stats;
    return 0;
}

