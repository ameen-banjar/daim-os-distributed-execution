#ifndef OVS_SWITCH_ADAPTER_H
#define OVS_SWITCH_ADAPTER_H

#include "daim_switch_adapter.h"

typedef int (*daim_ovs_executor)(void *context, const char *const argv[]);

int daim_ovs_adapter_create(struct daim_switch_adapter *adapter,
                            daim_ovs_executor executor,
                            void *executor_context);

/* Production executor: starts argv[0] directly without a shell. */
int daim_ovs_spawn_executor(void *context, const char *const argv[]);

#endif

