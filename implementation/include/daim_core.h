#ifndef DAIM_CORE_H
#define DAIM_CORE_H

#include <stddef.h>
#include <stdint.h>

#include "daim_os_api.h"

#ifdef __cplusplus
extern "C" {
#endif

enum daim_core_status {
    DAIM_CORE_OK = 0,
    DAIM_CORE_ERROR = 1
};

/* Internal runtime hook; not part of the v1.0.0 public interface. */
uint16_t daim_core_emit(uint16_t sig_type, void *data);

/* Test and monitoring helpers. */
size_t daim_core_table_count(uint8_t table);
uint64_t daim_core_generation(void);

#ifdef __cplusplus
}
#endif

#endif

