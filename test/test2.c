#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>

#define USE_SPALL 0

#if USE_SPALL
#define SPALL_AUTO_IMPLEMENTATION
#include "spall_native_auto.h"
#else
#define spall_auto_buffer_begin(...)
#define spall_auto_buffer_end(...)
#endif

static int num_threads;

static uint32_t my_hash(const void* a) {
    uint32_t x = (uint32_t) (uintptr_t) a;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = (x >> 16) ^ x;
    return x;
}

static bool my_cmp(const void* a, const void* b) {
    uint32_t x = (uint32_t) (uintptr_t) a;
    uint32_t y = (uint32_t) (uintptr_t) b;
    return x == y;
}

#define EBR_IMPL
#include "../ebr.h"

#define NBHM_IMPL
#define NBHM_FN(n) my_ ## n
#include "../nbhm.h"

typedef struct {
    #ifdef _WIN32
    CRITICAL_SECTION lock;
    #else
    pthread_mutex_t lock;
    #endif

    size_t exp;
    void* data[];
} LockedHS;

void* lhs_intern(LockedHS* hs, void* val) {
    EBR__BEGIN("intern");

    if (num_threads > 1) {
        #ifdef _WIN32
        EnterCriticalSection(&hs->lock);
        #else
        pthread_mutex_lock(&hs->lock);
        #endif
    }

    // actually lookup & insert
    uint32_t exp = hs->exp;
    size_t mask = (1 << exp) - 1;

    void* result = NULL;
    uint32_t h = my_hash(val);
    size_t first = h & mask, i = first;
    do {
        if (hs->data[i] == NULL) {
            hs->data[i] = result = val;
            break;
        } else if (my_cmp(hs->data[i], val)) {
            result = hs->data[i];
            break;
        }
        i = (i + 1) & mask;
    } while (i != first);
    assert(result != NULL);

    if (num_threads > 1) {
        #ifdef _WIN32
        LeaveCriticalSection(&hs->lock);
        #else
        pthread_mutex_unlock(&hs->lock);
        #endif
    }

    EBR__END();
    return result;
}

// https://github.com/demetri/scribbles/blob/master/randomness/prngs.c
uint32_t pcg32_pie(uint64_t *state) {
    uint64_t old = *state ^ 0xc90fdaa2adf85459ULL;
    *state = *state * 6364136223846793005ULL + 0xc90fdaa2adf85459ULL;
    uint32_t xorshifted = ((old >> 18u) ^ old) >> 27u;
    uint32_t rot = old >> 59u;
    return (xorshifted >> rot) | (xorshifted << ((-rot) & 31));
}

static LockedHS* test_lhs;
static NBHM test_set;

static int attempts; // per thread
static bool testing_lhs;

static int* thread_stats;
static _Atomic uint64_t total_time;

static uint64_t get_nanos(void) {
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static uint32_t current_thread_id(void) {
    #if _WIN32
    return GetCurrentThreadId();
    #else
    return pthread_self();
    #endif
}

static int rounds;
static int test_thread_fn(void* arg) {
    uintptr_t starting_id = (uintptr_t) arg;

    uint64_t full_id = (rounds*num_threads + starting_id + 1) << 56ull;
    uint64_t seed    = full_id * 11400714819323198485ULL;

    int* stats = &thread_stats[starting_id*16];
    stats[0] = stats[1] = stats[2] = 0;

    // printf("Launched! %zu\n", 1+starting_id);

    #if USE_SPALL
    spall_auto_thread_init(1+starting_id, SPALL_DEFAULT_BUFFER_SIZE);
    spall_auto_buffer_begin("work", 4, NULL, 0);
    #endif

    uint64_t start = get_nanos();
    if (testing_lhs) {
        abort();
    } else {
        for (size_t i = 0; i < attempts; i++) {
            uintptr_t k = (uintptr_t) pcg32_pie(&seed) & 0xFFFFF;
            void* key = (void*) (k + 1);
            if (my_put_if_null(&test_set, key, key) == NULL) {
                stats[0] += 1; // insertions
            } else {
                stats[1] += 1; // duplicate
            }

            for (size_t j = 0; j < 4; j++) {
                uintptr_t k = (uintptr_t) pcg32_pie(&seed) & 0xFFFFF;
                void* key = (void*) (k + 1);
                void* val = my_get(&test_set, key);
                if (val == NULL) {
                    stats[3] += 1;
                }
                stats[2] += 1; // find
            }
        }
    }
    total_time += get_nanos() - start;

    #if USE_SPALL
    spall_auto_buffer_end();
    spall_auto_thread_quit();
    #endif

    return 0;
}

int main(int argc, char** argv) {
    #if USE_SPALL
    spall_auto_init((char *)"profile.spall");
    spall_auto_thread_init(0, SPALL_DEFAULT_BUFFER_SIZE);
    #endif

    num_threads = atoi(argv[1]);
    // printf("Testing with %d threads\n", num_threads);

    if (argc >= 3 && strcmp(argv[2], "lhs") == 0) {
        testing_lhs = true;
        printf("  With Locked hashset...\n");
    }

    attempts     = 5000000 / num_threads;
    thread_stats = calloc(num_threads, 64 * sizeof(int));

    if (testing_lhs) {
        test_lhs = calloc(sizeof(LockedHS) + 262144*sizeof(void*), 1);
        test_lhs->exp = 18;

        #ifdef _WIN32
        InitializeCriticalSection(&test_lhs->lock);
        #endif
    } else {
        test_set = nbhm_alloc(32);
    }

    #if 1

    #else
    for (int j = 0; j < 5; j++) {
        total_time = 0;
        thrd_t* arr = malloc(num_threads * sizeof(thrd_t));

        uint64_t start = get_nanos();
        for (int i = 0; i < num_threads; i++) {
            thrd_create(&arr[i], test_thread_fn, (void*) (uintptr_t) i);
        }
        for (int i = 0; i < num_threads; i++) {
            thrd_join(arr[i], NULL);
        }
        uint64_t st_time = get_nanos() - start;

        int ins = 0, dup = 0, get = 0;
        for (int i = 0; i < num_threads; i++) {
            ins += thread_stats[16*i];
            dup += thread_stats[16*i + 1];
            get += thread_stats[16*i + 2];
        }

        double ops = ins + dup + get;
        double total_secs = st_time / 1000000000.0;
        printf("%.4f ns/op (total=%.4f ms), %.4f Mops/s (%.4f Mops)\n", total_time / ops, st_time / 1000000.0, (ops / total_secs) / 1000000.0, ops / 1000000.0);
        printf("  %d inserts, %d duplicates\n", ins, dup);
        rounds++;
    }
    #endif

    #if USE_SPALL
    spall_auto_thread_quit();
    spall_auto_quit();
    #endif

    return 0;
}

#if USE_SPALL
#define SPALL_AUTO_IMPLEMENTATION
#include "spall_native_auto.h"
#endif
