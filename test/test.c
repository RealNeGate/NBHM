#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdbool.h>
#include <threads.h>
#include <inttypes.h>
#include <stdatomic.h>

#define USE_SPALL 0

#if USE_SPALL
#define SPALL_AUTO_IMPLEMENTATION
#include "spall_native_auto.h"
#else
#define spall_auto_buffer_begin(...)
#define spall_auto_buffer_end(...)
#endif

#define EBR_IMPL
#include "../ebr.h"

// https://github.com/demetri/scribbles/blob/master/randomness/prngs.c
uint32_t pcg32_pie(uint64_t *state) {
    uint64_t old = *state ^ 0xc90fdaa2adf85459ULL;
    *state = *state * 6364136223846793005ULL + 0xc90fdaa2adf85459ULL;
    uint32_t xorshifted = ((old >> 18u) ^ old) >> 27u;
    uint32_t rot = old >> 59u;
    return (xorshifted >> rot) | (xorshifted << ((-rot) & 31));
}

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

typedef struct {
    int local_id;
    uint64_t total_time;

    // local op counts
    uint64_t ops[16];

    // local histograms
    uint64_t histo[256];
} HarnessState;

static const char* OP_NAMES[16];

// Run once (on one thread) before everything
static void test_init(void);

// All init_task() finish before run_task() begin
static void test_init_task(HarnessState* state);

// Actual test, part that's actually measured
static void test_run_task(HarnessState* state);

static void test_histo_put(HarnessState* state, int key) {
    state->histo[key > 255 ? 255 : key] += 1;
}

static atomic_int threads_ready;
static int num_threads;

static int test_harness(void* arg) {
    HarnessState* state = arg;

    // barrier
    ++threads_ready;
    while (threads_ready != num_threads) {
        thrd_yield();
    }

    uint64_t start = get_nanos();
    test_run_task(state);
    state->total_time += get_nanos() - start;
    return 0;
}

/* #if USE_SPALL
spall_auto_thread_init(1+starting_id, SPALL_DEFAULT_BUFFER_SIZE);
spall_auto_buffer_begin("work", 4, NULL, 0);
#endif

#if USE_SPALL
spall_auto_buffer_end();
spall_auto_thread_quit();
#endif */

static _Atomic int histo[256*16];
int main(int argc, char** argv) {
    #if USE_SPALL
    spall_auto_init((char *)"profile.spall");
    spall_auto_thread_init(0, SPALL_DEFAULT_BUFFER_SIZE);
    #endif

    num_threads = atoi(argv[1]);
    test_init();

    thrd_t* arr = malloc(num_threads * sizeof(thrd_t));
    HarnessState* harness = malloc(num_threads * sizeof(HarnessState));

    uint64_t start = get_nanos();
    for (int i = 0; i < num_threads; i++) {
        harness[i] = (HarnessState){ .local_id = i };
        thrd_create(&arr[i], test_harness, &harness[i]);
    }

    for (int i = 0; i < num_threads; i++) {
        thrd_join(arr[i], NULL);
    }
    // uint64_t st_time = get_nanos() - start;

    uint64_t st_time = 0;
    for (int i = 0; i < num_threads; i++) {
        st_time += harness[i].total_time;
    }
    st_time /= num_threads;

    for (int i = 0; i < 256; i++) {
        if (histo[i*16]) { printf("%d;%d\n", i, histo[i*16]); }
    }

    // dump histogram
    printf("\nHISTOGRAM!\n");
    for (int j = 0; j < 256; j++) {
        uint64_t sum = 0;
        for (int i = 0; i < num_threads; i++) {
            sum += harness[i].histo[j];
        }

        if (sum) {
            printf("%d;%"PRIu64"\n", j, sum);
        }
    }
    printf("\n\n");

    double total_secs  = st_time / 1000000000.0;
    uint64_t total_ops = 0;
    for (int j = 0; j < 16; j++) {
        if (OP_NAMES[j] == NULL) {
            continue;
        }

        uint64_t ops = 0;
        for (int i = 0; i < num_threads; i++) {
            ops += harness[i].ops[j];
        }
        printf("[%-15s] %10zu ops\n", OP_NAMES[j], ops);
        total_ops += ops;
    }
    printf("[%-15s] %.4f ns/op (total=%.4f ms), %.4f Mops/s (%.4f Mops)\n", "TOTAL", st_time / (double) total_ops, st_time / 1000000.0, (total_ops / total_secs) / 1000000.0, total_ops / 1000000.0);

    #if USE_SPALL
    spall_auto_thread_quit();
    spall_auto_quit();
    #endif

    return 0;
}

#if 1
#include "inserts.h"
#else
#include "lru.h"
#endif

#if USE_SPALL
#define SPALL_AUTO_IMPLEMENTATION
#include "spall_native_auto.h"
#endif
