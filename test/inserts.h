
static uint32_t my_hash(const void* a) {
    uint32_t x = (uint32_t) (uintptr_t) a;
    x ^= x >> 16;
    x *= 0x85ebca6bU;
    x ^= x >> 13;
    x *= 0xc2b2ae35U;
    x ^= x >> 16;
    return x;
}

static bool my_cmp(const void* a, const void* b) {
    uint32_t x = (uint32_t) (uintptr_t) a;
    uint32_t y = (uint32_t) (uintptr_t) b;
    return x == y;
}

#define NBHM_IMPL
#define NBHM_FN(n) my_ ## n
#include "../nbhm.h"

static int attempts;
static NBHM test_set;

static const char* OP_NAMES[16] = {
    "INSERT", "DUPLICATES", "REMOVE", "FOUND"
};

static void test_init(void) {
    attempts = 1000000 / num_threads;
    test_set = nbhm_alloc(1024);
}

static void test_init_task(HarnessState* state) {

}

static void test_run_task(HarnessState* state) {
    uint64_t full_id = (state->local_id + 1ull) << 56ull;
    uint64_t seed    = full_id * 11400714819323198485ULL;
    uint64_t seed2   = seed;

    for (int i = 0; i < 15000; i++) {
        pcg32_pie(&seed);
    }

    uint64_t* stats = state->ops;
    for (int i = 0; i < attempts; i++) {
        uintptr_t k = (uintptr_t) pcg32_pie(&seed) & 0x7FFFFFFF;
        void* key = (void*) (k + 3);
        if (my_put_if_null(&test_set, key, key) != NULL) {
            stats[1] += 1; // inserts
        }
        stats[0] += 1; // insertions

        if ((k % 31) > 21) {
            uintptr_t k = (uintptr_t) pcg32_pie(&seed2) & 0x7FFFFFFF;
            void* key = (void*) (k + 3);
            if (my_remove(&test_set, key) != NULL) {
                stats[3] += 1; // match
            }
            stats[2] += 1; // lookups
        }
    }
}

