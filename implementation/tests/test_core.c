#include "daim_core.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

static int callback_count;
static void on_signal(uint16_t signal, void *data) { assert(signal == STATE_CHANGE); assert(data != NULL); callback_count++; }

int main(void)
{
    struct switch_config_table_entry a = { SWITCH_UP | NOR_MOD, 3600, 1000000 };
    struct switch_config_table_entry b = { SWITCH_DOWN, 0, 0 };
    struct switch_config_table_entry *read;
    assert(daim_init() == DAIM_CORE_OK);
    assert(daim_table_write(DAIM_INFO_TABLE, &a, sizeof(a), ADD) == DAIM_CORE_ERROR);
    assert(daim_table_write(DAIM_SWITCH_CONFIG_TABLE, &a, sizeof(a), 0xff) == DAIM_CORE_ERROR);
    assert(daim_table_write(DAIM_SWITCH_CONFIG_TABLE, &a, sizeof(a), ADD) == DAIM_CORE_OK);
    assert(daim_table_write(DAIM_SWITCH_CONFIG_TABLE, &b, sizeof(b), ADD) == DAIM_CORE_OK);
    assert(daim_core_table_count(DAIM_SWITCH_CONFIG_TABLE) == 2);
    assert(daim_core_generation() == 2);
    read = daim_table_read(DAIM_SWITCH_CONFIG_TABLE, NULL, 0); assert(read && memcmp(read,&a,sizeof(a)) == 0); free(read);
    read = daim_table_read(DAIM_SWITCH_CONFIG_TABLE, NULL, 0); assert(read && memcmp(read,&b,sizeof(b)) == 0); free(read);
    assert(daim_table_read(DAIM_SWITCH_CONFIG_TABLE, NULL, 0) == NULL);
    daim_table_rewind(DAIM_SWITCH_CONFIG_TABLE);
    read = daim_table_read(DAIM_SWITCH_CONFIG_TABLE, &b, sizeof(b)); assert(read && memcmp(read,&b,sizeof(b)) == 0); free(read);
    assert(daim_table_write(DAIM_SWITCH_CONFIG_TABLE, &a, sizeof(a), DEL) == DAIM_CORE_OK);
    assert(daim_core_table_count(DAIM_SWITCH_CONFIG_TABLE) == 1);
    daim_signal(STATE_CHANGE, on_signal);
    assert(daim_core_emit(STATE_CHANGE, &b) == DAIM_CORE_OK);
    assert(callback_count == 1);
    daim_quit();
    return 0;
}

