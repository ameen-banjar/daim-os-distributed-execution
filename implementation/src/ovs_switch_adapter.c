#include "ovs_switch_adapter.h"

#include <spawn.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

extern char **environ;

struct ovs_context {
    daim_ovs_executor executor;
    void *executor_context;
};

static int unsupported_read(struct daim_switch_adapter *a, const uint8_t m[6], void *b, uint64_t s) { (void)a;(void)m;(void)b;(void)s; return -1; }
static int unsupported_write(struct daim_switch_adapter *a, const uint8_t m[6], const void *b, uint64_t s) { (void)a;(void)m;(void)b;(void)s; return -1; }
static int unsupported_ioctl(struct daim_switch_adapter *a, uint64_t r, void *d) { (void)a;(void)r;(void)d; return -1; }

static int valid_token(const char *value, size_t max)
{
    size_t len;
    if (!value) return 0;
    len = strlen(value);
    return len > 0 && len <= max && strchr(value, '\n') == NULL && strchr(value, '\r') == NULL;
}

static int ovs_flow(struct daim_switch_adapter *adapter, const char *command, const char *bridge, const char *flow)
{
    struct ovs_context *ctx = adapter->context;
    const char *argv[7];
    if (!valid_token(bridge, 63) || !valid_token(flow, 511)) return -1;
    argv[0] = "ovs-ofctl";
    argv[1] = "-O";
    argv[2] = "OpenFlow13";
    argv[3] = command;
    argv[4] = bridge;
    argv[5] = flow;
    argv[6] = NULL;
    return ctx->executor(ctx->executor_context, argv);
}

static int ovs_add(struct daim_switch_adapter *a, const char *b, const char *f) { return ovs_flow(a,"add-flow",b,f); }
static int ovs_delete(struct daim_switch_adapter *a, const char *b, const char *f) { return ovs_flow(a,"del-flows",b,f); }
static void ovs_destroy(struct daim_switch_adapter *a) { free(a->context); a->context=NULL; a->ops=NULL; }

static const struct daim_switch_adapter_ops ops = {
    unsupported_read, unsupported_write, unsupported_ioctl, ovs_add, ovs_delete, ovs_destroy
};

int daim_ovs_adapter_create(struct daim_switch_adapter *adapter, daim_ovs_executor executor, void *executor_context)
{
    struct ovs_context *ctx;
    if (!adapter || !executor) return -1;
    ctx = calloc(1, sizeof(*ctx)); if (!ctx) return -1;
    ctx->executor = executor; ctx->executor_context = executor_context;
    adapter->ops = &ops; adapter->context = ctx;
    return 0;
}

int daim_ovs_spawn_executor(void *context, const char *const argv[])
{
    pid_t pid; int status;
    (void)context;
    if (posix_spawnp(&pid, argv[0], NULL, NULL, (char *const *)argv, environ) != 0) return -1;
    if (waitpid(pid, &status, 0) < 0) return -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}
