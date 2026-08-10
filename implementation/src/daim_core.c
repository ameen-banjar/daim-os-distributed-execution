#include "daim_core.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define FIRST_TABLE DAIM_INFO_TABLE
#define LAST_TABLE DAIM_LINK_CONFIG_TABLE
#define TABLE_SLOTS ((LAST_TABLE - FIRST_TABLE) + 1)
#define SIGNAL_SLOTS (ENTITY_LEAVE + 1)

struct table_item {
    void *data;
    uint32_t size;
};

struct table_store {
    struct table_item *items;
    size_t count;
    size_t capacity;
    size_t cursor;
};

struct core_state {
    pthread_mutex_t lock;
    int lock_ready;
    int initialised;
    uint64_t generation;
    struct table_store tables[TABLE_SLOTS];
    sighandler signals[SIGNAL_SLOTS];
};

static struct core_state state;

static int valid_table(uint8_t table)
{
    return table >= FIRST_TABLE && table <= LAST_TABLE;
}

static int writable_table(uint8_t table)
{
    return table >= DAIM_PACKET_FORWARDING_TABLE && table <= DAIM_LINK_CONFIG_TABLE;
}

static struct table_store *store_for(uint8_t table)
{
    return valid_table(table) ? &state.tables[table - FIRST_TABLE] : NULL;
}

static void free_store(struct table_store *store)
{
    size_t i;
    for (i = 0; i < store->count; ++i) {
        free(store->items[i].data);
    }
    free(store->items);
    memset(store, 0, sizeof(*store));
}

uint16_t daim_init(void)
{
    if (!state.lock_ready) {
        if (pthread_mutex_init(&state.lock, NULL) != 0) {
            return DAIM_CORE_ERROR;
        }
        state.lock_ready = 1;
    }
    pthread_mutex_lock(&state.lock);
    if (!state.initialised) {
        memset(state.tables, 0, sizeof(state.tables));
        memset(state.signals, 0, sizeof(state.signals));
        state.generation = 0;
        state.initialised = 1;
    }
    pthread_mutex_unlock(&state.lock);
    return DAIM_CORE_OK;
}

void daim_quit(void)
{
    size_t i;
    if (!state.lock_ready) {
        return;
    }
    pthread_mutex_lock(&state.lock);
    for (i = 0; i < TABLE_SLOTS; ++i) {
        free_store(&state.tables[i]);
    }
    memset(state.signals, 0, sizeof(state.signals));
    state.initialised = 0;
    state.generation = 0;
    pthread_mutex_unlock(&state.lock);
}

static int ensure_capacity(struct table_store *store)
{
    size_t next;
    struct table_item *items;
    if (store->count < store->capacity) {
        return 1;
    }
    next = store->capacity ? store->capacity * 2 : 8;
    items = realloc(store->items, next * sizeof(*items));
    if (!items) {
        return 0;
    }
    store->items = items;
    store->capacity = next;
    return 1;
}

uint16_t daim_table_write(uint8_t table, void *entry, uint32_t size, uint8_t op_code)
{
    struct table_store *store;
    size_t i;
    void *copy;
    if (!state.lock_ready || !state.initialised || !writable_table(table) || !entry || size == 0) {
        return DAIM_CORE_ERROR;
    }
    if (op_code != ADD && op_code != DEL) {
        return DAIM_CORE_ERROR;
    }
    pthread_mutex_lock(&state.lock);
    store = store_for(table);
    if (op_code == ADD) {
        if (!ensure_capacity(store)) {
            pthread_mutex_unlock(&state.lock);
            return DAIM_CORE_ERROR;
        }
        copy = malloc(size);
        if (!copy) {
            pthread_mutex_unlock(&state.lock);
            return DAIM_CORE_ERROR;
        }
        memcpy(copy, entry, size);
        store->items[store->count].data = copy;
        store->items[store->count].size = size;
        store->count++;
    } else {
        for (i = 0; i < store->count; ++i) {
            if (store->items[i].size == size && memcmp(store->items[i].data, entry, size) == 0) {
                free(store->items[i].data);
                memmove(&store->items[i], &store->items[i + 1],
                        (store->count - i - 1) * sizeof(store->items[0]));
                store->count--;
                if (store->cursor > store->count) {
                    store->cursor = store->count;
                }
                state.generation++;
                pthread_mutex_unlock(&state.lock);
                return DAIM_CORE_OK;
            }
        }
        pthread_mutex_unlock(&state.lock);
        return DAIM_CORE_ERROR;
    }
    state.generation++;
    pthread_mutex_unlock(&state.lock);
    return DAIM_CORE_OK;
}

void *daim_table_read(uint8_t table, void *entry, uint32_t size)
{
    struct table_store *store;
    void *copy = NULL;
    size_t i;
    if (!state.lock_ready || !state.initialised || !valid_table(table)) {
        return NULL;
    }
    pthread_mutex_lock(&state.lock);
    store = store_for(table);
    for (i = store->cursor; i < store->count; ++i) {
        if (entry && (size > store->items[i].size || memcmp(store->items[i].data, entry, size) != 0)) {
            continue;
        }
        copy = malloc(store->items[i].size);
        if (copy) {
            memcpy(copy, store->items[i].data, store->items[i].size);
            store->cursor = i + 1;
        }
        break;
    }
    pthread_mutex_unlock(&state.lock);
    return copy;
}

void daim_table_rewind(uint8_t table)
{
    struct table_store *store;
    if (!state.lock_ready || !state.initialised || !valid_table(table)) {
        return;
    }
    pthread_mutex_lock(&state.lock);
    store = store_for(table);
    store->cursor = 0;
    pthread_mutex_unlock(&state.lock);
}

void daim_signal(uint16_t sig_type, sighandler handler)
{
    if (!state.lock_ready || !state.initialised || sig_type >= SIGNAL_SLOTS) {
        return;
    }
    pthread_mutex_lock(&state.lock);
    state.signals[sig_type] = handler;
    pthread_mutex_unlock(&state.lock);
}

uint16_t daim_core_emit(uint16_t sig_type, void *data)
{
    sighandler handler;
    if (!state.lock_ready || !state.initialised || sig_type >= SIGNAL_SLOTS) {
        return DAIM_CORE_ERROR;
    }
    pthread_mutex_lock(&state.lock);
    handler = state.signals[sig_type];
    pthread_mutex_unlock(&state.lock);
    if (!handler) {
        return DAIM_CORE_ERROR;
    }
    handler(sig_type, data);
    return DAIM_CORE_OK;
}

size_t daim_core_table_count(uint8_t table)
{
    size_t count = 0;
    if (!state.lock_ready || !state.initialised || !valid_table(table)) {
        return 0;
    }
    pthread_mutex_lock(&state.lock);
    count = store_for(table)->count;
    pthread_mutex_unlock(&state.lock);
    return count;
}

uint64_t daim_core_generation(void)
{
    uint64_t generation = 0;
    if (!state.lock_ready || !state.initialised) {
        return 0;
    }
    pthread_mutex_lock(&state.lock);
    generation = state.generation;
    pthread_mutex_unlock(&state.lock);
    return generation;
}

