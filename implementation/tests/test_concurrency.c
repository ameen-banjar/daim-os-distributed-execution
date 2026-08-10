#include "daim_core.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

enum {
    WRITERS = 4,
    READERS = 3,
    WRITES_PER_THREAD = 1000,
    DELETE_STRIDE = 2,
    SIGNAL_THREADS = 4,
    SIGNALS_PER_THREAD = 2000
};

static atomic_int writers_done;
static atomic_uint_fast64_t callback_count;

static struct switch_link_config_table_entry make_entry(uint64_t id)
{
    struct switch_link_config_table_entry entry = {0};
    entry.id = id;
    entry.link_state = LINK_UP;
    entry.link_speed = 1000000000ULL;
    entry.weight = (uint8_t)(id % 101);
    return entry;
}

static void *writer(void *argument)
{
    uintptr_t thread_no = (uintptr_t)argument;
    int i;
    for (i = 0; i < WRITES_PER_THREAD; ++i) {
        uint64_t id = (uint64_t)(thread_no * WRITES_PER_THREAD + (uintptr_t)i + 1);
        struct switch_link_config_table_entry entry = make_entry(id);
        assert(daim_table_write(DAIM_LINK_CONFIG_TABLE, &entry, sizeof(entry), ADD) == DAIM_CORE_OK);
    }
    atomic_fetch_add_explicit(&writers_done, 1, memory_order_release);
    return NULL;
}

static void *reader(void *argument)
{
    (void)argument;
    while (atomic_load_explicit(&writers_done, memory_order_acquire) < WRITERS) {
        struct switch_link_config_table_entry *copy;
        daim_table_rewind(DAIM_LINK_CONFIG_TABLE);
        copy = daim_table_read(DAIM_LINK_CONFIG_TABLE, NULL, 0);
        if (copy) {
            assert(copy->link_state == LINK_UP);
            free(copy);
        }
        (void)daim_core_table_count(DAIM_LINK_CONFIG_TABLE);
        (void)daim_core_generation();
    }
    return NULL;
}

static void *deleter(void *argument)
{
    uint64_t id;
    (void)argument;
    while (atomic_load_explicit(&writers_done, memory_order_acquire) < WRITERS) {
        (void)daim_core_table_count(DAIM_LINK_CONFIG_TABLE);
    }
    for (id = 1; id <= (uint64_t)(WRITERS * WRITES_PER_THREAD); id += DELETE_STRIDE) {
        struct switch_link_config_table_entry entry = make_entry(id);
        assert(daim_table_write(DAIM_LINK_CONFIG_TABLE, &entry, sizeof(entry), DEL) == DAIM_CORE_OK);
    }
    return NULL;
}

static void callback(uint16_t sig_type, void *data)
{
    assert(sig_type == NO_RULE);
    assert(data != NULL);
    atomic_fetch_add_explicit(&callback_count, 1, memory_order_relaxed);
}

static void *emitter(void *argument)
{
    uintptr_t token = (uintptr_t)argument + 1;
    int i;
    for (i = 0; i < SIGNALS_PER_THREAD; ++i) {
        assert(daim_core_emit(NO_RULE, &token) == DAIM_CORE_OK);
    }
    return NULL;
}

int main(void)
{
    pthread_t writers[WRITERS];
    pthread_t readers[READERS];
    pthread_t delete_thread;
    pthread_t signal_threads[SIGNAL_THREADS];
    uintptr_t i;
    const size_t writes = WRITERS * WRITES_PER_THREAD;
    const size_t deletes = writes / DELETE_STRIDE;

    atomic_init(&writers_done, 0);
    atomic_init(&callback_count, 0);
    assert(daim_init() == DAIM_CORE_OK);

    for (i = 0; i < READERS; ++i) {
        assert(pthread_create(&readers[i], NULL, reader, NULL) == 0);
    }
    for (i = 0; i < WRITERS; ++i) {
        assert(pthread_create(&writers[i], NULL, writer, (void *)i) == 0);
    }
    assert(pthread_create(&delete_thread, NULL, deleter, NULL) == 0);
    for (i = 0; i < WRITERS; ++i) assert(pthread_join(writers[i], NULL) == 0);
    for (i = 0; i < READERS; ++i) assert(pthread_join(readers[i], NULL) == 0);
    assert(pthread_join(delete_thread, NULL) == 0);

    assert(daim_core_table_count(DAIM_LINK_CONFIG_TABLE) == writes - deletes);
    assert(daim_core_generation() == writes + deletes);

    daim_signal(NO_RULE, callback);
    for (i = 0; i < SIGNAL_THREADS; ++i) {
        assert(pthread_create(&signal_threads[i], NULL, emitter, (void *)i) == 0);
    }
    for (i = 0; i < SIGNAL_THREADS; ++i) assert(pthread_join(signal_threads[i], NULL) == 0);
    assert(atomic_load_explicit(&callback_count, memory_order_relaxed) ==
           SIGNAL_THREADS * SIGNALS_PER_THREAD);

    daim_quit();
    return 0;
}
