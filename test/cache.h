// This test is modelled after a simple ARP cache, we're mapping random
// MAC-to-IP entries. This is a read-heavy test, each connection will
// perform around 200-1000 lookups. There are usually 200K entries but
// we could tweak that.
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

// #define MC_TESTING
// #define MC_IMPL

#define NBHM_IMPL
#define NBHM_FN(n) my_ ## n
#include "../nbhm.h"

static int attempts;
static NBHM test_set;

typedef struct {
    _Atomic(int) remaining;
} Client;

static Client* clients;

static const char* OP_NAMES[16] = {
    "CONNECT", "DISCONNECT", "LOOKUP"
};

static void test_init(void) {
    attempts = 100000000 / num_threads;
    test_set = nbhm_alloc(256);
    clients  = calloc(0x3FFFF+1, sizeof(Client));
}

static void test_init_task(HarnessState* state) {
}

static void test_run_task(HarnessState* state) {
    uint64_t full_id = (state->local_id + 1ull) << 56ull;
    uint64_t seed    = full_id * 11400714819323198485ULL;
    uint64_t* stats  = state->ops;

    #ifdef MC_TESTING
    my_ticket = state->local_id;
    mc_seed = seed;

    while (active_ticket != my_ticket) {
        thrd_yield();
    }

    for (int i = 0; i < attempts; i++) {
        uint64_t k = pcg32_pie(&seed) & 0xFFFF;
        uint64_t v = pcg32_pie(&seed) & 0xFFFFFFFF;

        if ((state->local_id) & 1) {
            my_get(&test_set, (void*) (k + 2));
        } else {
            my_put_if_null(&test_set, (void*) (k + 2), (void*) (v + 3));
        }
    }
    #else
    for (int i = 0; i < attempts; i++) {
        // Pick a random client, if there's no remaining tasks then
        // connect and assign it another pool of tasks.
        uint32_t id = (uint32_t) pcg32_pie(&seed) & 0x3FFFF;

        int curr = atomic_load(&clients[id].remaining);
        int next = 0;
        do {
            next = curr ? curr - 1 : ((pcg32_pie(&seed) % 15) + 15);
        } while (!atomic_compare_exchange_strong(&clients[id].remaining, &curr, next));

        // uint64_t start = get_nanos();
        uint64_t fake_mac = id + 3;
        if (curr == 0) {
            my_put_if_null(&test_set, (void*) fake_mac, &clients[id]);
            stats[0] += 1; // connect
        } else if (next == 0) {
            my_remove(&test_set, (void*) fake_mac);
            stats[1] += 1; // disconnect
        } else {
            if (my_get(&test_set, (void*) fake_mac) == &clients[id]) {
                stats[2] += 1; // lookups
            }
        }
        // test_histo_put(state, get_nanos() - start);
    }
    #endif
}

