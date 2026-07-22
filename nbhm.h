////////////////////////////////
// NBHM - Non-blocking hashmap
////////////////////////////////
// You wanna intern lots of things on lots of cores? this is for you. It's
// inspired by Cliff's non-blocking hashmap.
//
// To use it, you'll need to define NBHM_FN and then include the header:
//
//   #define NBHM_FN(n) XXX_hm_ ## n
//   #include <nbhm.h>
//
// This will compile implementations of the hashset using
//
//   bool NBHM_FN(cmp)(const void* a, const void* b);
//   uint32_t NBHM_FN(hash)(const void* a);
//
// The exported functions are:
//
//   void* NBHM_FN(get)(NBHM* hm, void* key);
//   void* NBHM_FN(put)(NBHM* hm, void* key, void* val);
//   void* NBHM_FN(put_if_null)(NBHM* hm, void* key, void* val);
//   void NBHM_FN(resize_barrier)(NBHM* hm);
//
#ifndef NBHM_H
#define NBHM_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <assert.h>
#include <stdalign.h>

#ifdef __cplusplus
#define _Atomic(x) std::atomic<x>
#include <atomic>
#else
#include <stdatomic.h>
#endif

#include "ebr.h"

enum {
    NBHM_PROBE_MIN_LIMIT = 20,

    NBHM_LOAD_FACTOR = 75,
    NBHM_MOVE_AMOUNT = 1024,
};

typedef struct {
    _Atomic(void*) key;
    _Atomic(void*) val;
} NBHM_Entry;

typedef struct {
    #if 1
    alignas(64) struct {
        _Atomic uint64_t last_update;
        _Atomic uint64_t last_sum;
    };

    // 4-way striping, eventually consistent
    alignas(64) _Atomic(uint32_t) count[4 * 16];
    #else
    _Atomic(uint32_t) count;
    #endif
} NBHM_Counter;

typedef struct NBHM_Table NBHM_Table;
struct NBHM_Table {
    _Atomic(NBHM_Table*) next;

    uint32_t cap;

    // reciprocals to compute modulo
    uint64_t a, sh;

    // tracks how many entries have
    // been moved once we're resizing
    alignas(64) _Atomic(uint32_t) moved;
    alignas(64) _Atomic(uint32_t) move_done;

    NBHM_Counter slots; // claimed slots
    NBHM_Counter count; // actual entries

    NBHM_Entry data[];
};

typedef struct {
    #ifdef __cplusplus
    NBHM_Table* curr;
    #else
    _Atomic(NBHM_Table*) curr;
    #endif
} NBHM;

static size_t nbhm_compute_cap(size_t y) {
    // minimum capacity
    if (y < 256) {
        y = 256;
    } else {
        y = ((y + 1) / 3) * 4;
    }

    size_t cap = 1ull << (64 - __builtin_clzll(y - 1));
    return cap - (sizeof(NBHM_Table) / sizeof(NBHM_Entry));
}

#ifndef NEGATE__DIV128_IMPL
#define NEGATE__DIV128_IMPL

// (X + Y) / Z = int(X/Z) + int(Y/Z) + (mod(X,Z) + mod(Y,Z)/Z
static uint64_t negate__div128(uint64_t numhi, uint64_t numlo, uint64_t den, uint64_t* out_rem) {
    // https://github.com/ridiculousfish/libdivide/blob/master/libdivide.h (libdivide_128_div_64_to_64)
    //
    // We work in base 2**32.
    // A uint32 holds a single digit. A uint64 holds two digits.
    // Our numerator is conceptually [num3, num2, num1, num0].
    // Our denominator is [den1, den0].
    const uint64_t b = ((uint64_t)1 << 32);

    // Check for overflow and divide by 0.
    if (numhi >= den) {
        if (out_rem) *out_rem = ~0ull;
        return ~0ull;
    }

    // Determine the normalization factor. We multiply den by this, so that its leading digit is at
    // least half b. In binary this means just shifting left by the number of leading zeros, so that
    // there's a 1 in the MSB.
    // We also shift numer by the same amount. This cannot overflow because numhi < den.
    // The expression (-shift & 63) is the same as (64 - shift), except it avoids the UB of shifting
    // by 64. The funny bitwise 'and' ensures that numlo does not get shifted into numhi if shift is
    // 0. clang 11 has an x86 codegen bug here: see LLVM bug 50118. The sequence below avoids it.
    int shift = __builtin_clzll(den) - 1;
    den <<= shift;
    numhi <<= shift;
    numhi |= (numlo >> (-shift & 63)) & (uint64_t)(-(int64_t)shift >> 63);
    numlo <<= shift;

    // Extract the low digits of the numerator and both digits of the denominator.
    uint32_t num1 = (uint32_t)(numlo >> 32);
    uint32_t num0 = (uint32_t)(numlo & 0xFFFFFFFFu);
    uint32_t den1 = (uint32_t)(den >> 32);
    uint32_t den0 = (uint32_t)(den & 0xFFFFFFFFu);

    // We wish to compute q1 = [n3 n2 n1] / [d1 d0].
    // Estimate q1 as [n3 n2] / [d1], and then correct it.
    // Note while qhat may be 2 digits, q1 is always 1 digit.
    uint64_t qhat = numhi / den1;
    uint64_t rhat = numhi % den1;
    uint64_t c1 = qhat * den0;
    uint64_t c2 = rhat * b + num1;
    if (c1 > c2) qhat -= (c1 - c2 > den) ? 2 : 1;
    uint32_t q1 = (uint32_t)qhat;

    // Compute the true (partial) remainder.
    uint64_t rem = numhi * b + num1 - q1 * den;

    // We wish to compute q0 = [rem1 rem0 n0] / [d1 d0].
    // Estimate q0 as [rem1 rem0] / [d1] and correct it.
    qhat = rem / den1;
    rhat = rem % den1;
    c1 = qhat * den0;
    c2 = rhat * b + num0;
    if (c1 > c2) qhat -= (c1 - c2 > den) ? 2 : 1;
    uint32_t q0 = (uint32_t)qhat;

    // Return remainder if requested.
    if (out_rem) *out_rem = (rem * b + num0 - q0 * den) >> shift;
    return ((uint64_t)q1 << 32) | q0;
}
#endif /* NEGATE__DIV128_IMPL */

static void nbhm_compute_size(NBHM_Table* table, size_t cap) {
    // reciprocals to compute modulo
    #if defined(__GNUC__) || defined(__clang__)
    table->sh = 64 - __builtin_clzll(cap);
    #else
    uint64_t sh = 0;
    while (cap > (1ull << sh)){ sh++; }
    table->sh = sh;
    #endif

    table->sh += 63 - 64;
    table->a = negate__div128(1ull << table->sh, cap - 1, cap, NULL);

    #if (defined(__GNUC__) || defined(__clang__)) && defined(__x86_64__)
    uint64_t d,e;
    __asm__("div %[v]" : "=a"(d), "=d"(e) : [v] "r"(cap), "a"(cap - 1), "d"(1ull << table->sh));
    assert(d == table->a);
    #endif

    table->cap = cap;
}

static NBHM nbhm_alloc(size_t initial_cap) {
    ebr_init();

    size_t cap = nbhm_compute_cap(initial_cap);
    NBHM_Table* table = (NBHM_Table*) EBR_VIRTUAL_ALLOC(sizeof(NBHM_Table) + cap*sizeof(NBHM_Entry));
    nbhm_compute_size(table, cap);

    #ifdef __cplusplus
    return { table };
    #else
    return (NBHM){ table };
    #endif
}

static void nbhm_free(NBHM* hs) {
    NBHM_Table* curr = hs->curr;
    while (curr) {
        NBHM_Table* next = curr->next;
        EBR_VIRTUAL_FREE(curr, sizeof(NBHM_Table) + curr->cap*sizeof(NBHM_Entry));
        curr = next;
    }
}

// for spooky stuff
/* static NBHM_Entry* nbhm_array(NBHM* hs) { return hs->curr->data; }
static size_t nbhm_count(NBHM* hs)      { return hs->curr->count; }
static size_t nbhm_capacity(NBHM* hs)   { return hs->curr->cap; }
#define nbhm_for(it, hs) for (NBHM_Entry *it = nbhm_array(hs), *_end_ = &it[nbhm_capacity(hs)]; it != _end_; it++) if (it->key != NULL && it->key != &NBHM_TOMBSTONE)
*/

#if defined(_WIN32)
#pragma comment(lib, "synchronization.lib")
#endif
#endif // NBHM_H

#ifdef NBHM_IMPL
#endif // NBHM_IMPL

// Templated implementation
#ifdef NBHM_FN
#include <x86intrin.h>

#define NBHM_TOMBSTONE    ((void*) 1)
#define NBHM_NO_MATCH_OLD ((void*) 2)

#define nbhm_ldacq(addr) atomic_load_explicit(addr, memory_order_acquire)
#define nbhm_strel(addr, x) atomic_store_explicit(addr, x, memory_order_release)
#define nbhm_cas(a, b, c) atomic_compare_exchange_strong_explicit(a, b, c, memory_order_acq_rel, memory_order_acquire)
#define nbhm_fetch_add(a, b) atomic_fetch_add_explicit(a, b, memory_order_acq_rel)

static void* NBHM_FN(put_if_match)(NBHM_Table* latest, void* key, void* val, void* exp);

static size_t NBHM_FN(hash2index)(NBHM_Table* table, uint64_t u) {
    uint64_t v = table->a;

    // Multiply high 64: Ripped, straight, from, Hacker's delight... mmm delight
    uint64_t u0 = u & 0xFFFFFFFF;
    uint64_t u1 = u >> 32;
    uint64_t v0 = v & 0xFFFFFFFF;
    uint64_t v1 = v >> 32;
    uint64_t w0 = u0*v0;
    uint64_t t = u1*v0 + (w0 >> 32);
    uint64_t w1 = (u0*v1) + (t & 0xFFFFFFFF);
    uint64_t w2 = (u1*v1) + (t >> 32);
    uint64_t hi = w2 + (w1 >> 32);
    // Modulo from quotient
    uint64_t q  = hi >> table->sh;
    uint64_t q2 = u - (q * table->cap);

    assert(q2 == u % table->cap);
    return q2;
}

static void NBHM_FN(counter_add)(NBHM_Counter* dst, uint32_t hash, int delta) {
    if (delta) {
        atomic_fetch_add_explicit(&dst->count[(hash & 3) * 16], delta, memory_order_acq_rel);
        // atomic_fetch_add_explicit(&dst->count, delta, memory_order_acq_rel);
    }
}

static uint32_t NBHM_FN(counter_get)(NBHM_Counter* dst) {
    uint32_t total = 0;
    for (int i = 0; i < 4; i++) {
        total += nbhm_ldacq(&dst->count[i * 16]);
    }
    return total;
    // return atomic_load_explicit(&dst->count, memory_order_acquire);
}

static _Atomic int histo[256*16];
static uint32_t NBHM_FN(counter_estimate)(NBHM_Counter* dst) {
    // uint64_t start = __rdtsc();

    #if 0
    uint64_t s = NBHM_FN(counter_get)(dst);
    #else
    // only update the estimate every millisecond
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t ticks = (ts.tv_sec * 1000L) + (ts.tv_nsec / 1000L);
    if (nbhm_ldacq(&dst->last_update) != ticks) {
        nbhm_strel(&dst->last_sum,    NBHM_FN(counter_get)(dst));
        nbhm_strel(&dst->last_update, ticks);
        histo[16]++;
    } else {
        histo[0]++;
    }
    uint64_t s = nbhm_ldacq(&dst->last_sum);
    #endif

    // uint64_t delta = __rdtsc() - start;
    // histo[(delta > 255 ? 255 : delta) * 16] += 1;
    return s;
}

static void NBHM_FN(migrate_item)(NBHM_Table* table, NBHM_Table* new_table, size_t i) {
    // CAS key: NULL -> TOMBSTONE to stop entries from claiming the key
    void* k = nbhm_ldacq(&table->data[i].key);
    while (k == NULL && !nbhm_cas(&table->data[i].key, &k, NBHM_TOMBSTONE)) {
        // ...
    }

    if (k == NULL) {
        return;
    }

    // freeze the values by adding a prime bit.
    void* old_v = nbhm_ldacq(&table->data[i].val);
    while (((uintptr_t) old_v & EBR_PRIME_BIT) == 0) {
        uintptr_t primed_v = (old_v == NBHM_TOMBSTONE ? 0 : (uintptr_t) old_v) | EBR_PRIME_BIT;
        if (atomic_compare_exchange_strong(&table->data[i].val, &old_v, (void*) primed_v)) {
            old_v = (void*) primed_v;
            break;
        }
        // btw, CAS updated old_v
    }

    // someone else has moved the value
    if (old_v == (void*) EBR_PRIME_BIT) {
        return;
    }

    // strip prime bit
    void* v = (void*) ((uintptr_t) old_v & ~EBR_PRIME_BIT);
    assert(v != NULL && v != NBHM_TOMBSTONE);
    NBHM_FN(put_if_match)(new_table, k, v, NBHM_NO_MATCH_OLD);

    // TODO(NeGate): we can replace the PRIME entry with a TOMBPRIME now that we've migrated it up.
    // ...
}

NBHM_Table* NBHM_FN(move_items)(NBHM* hm, NBHM_Table* top_table, NBHM_Table* old_table, int items_to_move) {
    assert(old_table);
    size_t cap = old_table->cap;

    // snatch up some number of items
    uint32_t old, new;
    do {
        old = nbhm_ldacq(&old_table->moved);
        if (old == cap) { return old_table; }
        // cap the number of items to copy... by the cap
        new = old + items_to_move;
        if (new > cap) { new = cap; }
    } while (!nbhm_cas(&old_table->moved, &(uint32_t){ old }, new));

    if (old == new) {
        return old_table;
    }

    EBR__BEGIN("copying old");
    for (size_t i = old; i < new; i++) {
        NBHM_FN(migrate_item)(old_table, top_table, i);
    }
    EBR__END();

    uint32_t done = nbhm_fetch_add(&old_table->move_done, new - old);
    done += new - old;

    // Replace the "freshest" known table with the new one, now that we've migrated all entries
    assert(done <= cap);
    if (done == cap && nbhm_cas(&hm->curr, &old_table, top_table)) {
        ebr_free(old_table, sizeof(NBHM_Table) + old_table->cap*sizeof(NBHM_Entry));
        return top_table;
    }

    return old_table;
}

static NBHM_Table* NBHM_FN(resize)(NBHM_Table* table, size_t limit) {
    NBHM_Table* next = nbhm_ldacq(&table->next);
    if (next != NULL) {
        return next;
    }

    // make resized table, we'll amortize the moves upward
    size_t new_cap = table->cap;
    if (NBHM_FN(counter_get)(&table->count) >= new_cap / 2) {
        new_cap = nbhm_compute_cap(limit * 3);
    }

    NBHM_Table* new_top = EBR_VIRTUAL_ALLOC(sizeof(NBHM_Table) + new_cap*sizeof(NBHM_Entry));
    nbhm_compute_size(new_top, new_cap);

    NBHM_Table* exp = NULL;
    if (!nbhm_cas(&table->next, &exp, new_top)) {
        EBR_VIRTUAL_FREE(new_top, sizeof(NBHM_Table) + new_cap*sizeof(NBHM_Entry));
        return exp;
    } else {
        // float s = sizeof(NBHM_Table) + new_cap*sizeof(NBHM_Entry);
        // printf("Resize: %p %.2f KiB (cap=%zu, %u %u)\n", new_top, s / 1024.0f, new_cap, NBHM_FN(counter_get)(&table->count), NBHM_FN(counter_get)(&table->slots));
        return new_top;
    }
}

// returns the old value, or NULL if there was none
static void* NBHM_FN(put_if_match)(NBHM_Table* table, void* key, void* val, void* exp) {
    assert(key && key != NBHM_TOMBSTONE);

    uint32_t h = NBHM_FN(hash)(key);
    NBHM_Table* next = NULL;
    void *k, *v;

    uint32_t cap = table->cap;
    uint32_t shift = __builtin_clz(cap - 1);
    uint32_t limit = (cap * NBHM_LOAD_FACTOR) / 100;
    uint32_t probe_limit = NBHM_PROBE_MIN_LIMIT + (cap >> 10u);

    // key claiming phase:
    //   once completed we'll have a key inserted into the latest
    //   table (the value might be NULL which means that the entry
    //   is still empty but we've at least agreed where the value
    //   goes).
    int probe = 1;
    bool found = false;
    size_t i = NBHM_FN(hash2index)(table, h);
    for (;;) {
        v = nbhm_ldacq(&table->data[i].val);
        k = nbhm_ldacq(&table->data[i].key);

        if (k == NULL) {
            // key was never in the table
            if (val == NBHM_TOMBSTONE) { return NULL; }

            // fight for empty slot
            if (nbhm_cas(&table->data[i].key, &k, key)) {
                NBHM_FN(counter_add)(&table->slots, h, 1);
                found = true;
                break;
            }
        }

        if (NBHM_FN(cmp)(k, key)) {
            found = true;
            break;
        }

        // if we reprobe enough, just make a new table.
        // if we see a tombstone then the table is currently mid-migration
        // so we might as well use the new one.
        if (++probe >= probe_limit || k == NBHM_TOMBSTONE) {
            // histo[(probe > 255 ? 255 : probe) * 16] += 1;
            NBHM_Table* next = NBHM_FN(resize)(table, limit);
            return NBHM_FN(put_if_match)(next, key, val, exp);
        }

        // mask-step-index
        i += (h >> shift) | 1;
        while (i >= cap) { i -= cap; }
    }

    // histo[(probe > 255 ? 255 : probe) * 16] += 1;
    for (;;) {
        // one CAS is slower than no CAS
        if (v == val) {
            return v;
        }

        if (next == NULL && ((v == NULL && NBHM_FN(counter_estimate)(&table->count) >= limit) || ((uintptr_t) v & EBR_PRIME_BIT))) {
            next = NBHM_FN(resize)(table, limit);
        }

        if (next != NULL) {
            NBHM_FN(migrate_item)(table, next, i);
            return NBHM_FN(put_if_match)(next, key, val, exp);
        }

        if (exp != NBHM_NO_MATCH_OLD &&
            exp != v &&
            !(exp == NBHM_TOMBSTONE && v == NULL)
        ) {
            return v == NBHM_TOMBSTONE ? NULL : v;
        }

        // value writing attempt, if we lose the CAS it means someone could've written a
        // prime (thus the entry was migrated to a later table). It could also mean we lost
        // the insertion fight to another writer and in that case we'll take their value.
        if (nbhm_cas(&table->data[i].val, &v, val)) {
            int delta = 0;
            if (exp != NULL) {
                if (v == NULL || v == NBHM_TOMBSTONE) {
                    if (val != NBHM_TOMBSTONE) { delta =  1; }
                } else {
                    if (val == NBHM_TOMBSTONE) { delta = -1; }
                }
                NBHM_FN(counter_add)(&table->count, h, delta);
            }
            return v;
        }

        // if we see a prime, the entry has been migrated
        // and we should write to that later table. if not,
        // we simply lost the race to update the value.
        uintptr_t v_raw = (uintptr_t) v;
        if (v_raw & EBR_PRIME_BIT) {
            NBHM_Table* next = nbhm_ldacq(&table->next);
            NBHM_FN(migrate_item)(table, next, i);
            return NBHM_FN(put_if_match)(next, key, val, exp);
        }
    }
}

static NBHM_Table* NBHM_FN(coop_migrate)(NBHM* hm) {
    // Migrate entries into the "next" table, once all are moved we
    // can just replace the current with it.
    NBHM_Table* curr = nbhm_ldacq(&hm->curr);
    NBHM_Table* next = nbhm_ldacq(&curr->next);
    if (next != NULL) {
        return NBHM_FN(move_items)(hm, next, curr, NBHM_MOVE_AMOUNT);
    }
    return curr;
}

void* NBHM_FN(put)(NBHM* hm, void* key, void* val) {
    EBR__BEGIN("put");

    assert(val);
    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    void* v = NBHM_FN(put_if_match)(curr, key, val, NBHM_NO_MATCH_OLD);
    ebr_exit_cs();
    EBR__END();
    return v;
}

void* NBHM_FN(remove)(NBHM* hm, void* key) {
    EBR__BEGIN("remove");

    assert(key);
    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    void* v = NBHM_FN(put_if_match)(curr, key, NBHM_TOMBSTONE, NBHM_NO_MATCH_OLD);
    ebr_exit_cs();
    EBR__END();
    return v;
}

static void* NBHM_FN(raw_lookup)(NBHM_Table* table, uint32_t h, void* key, void* prev_v) {
    uint32_t cap   = table->cap;
    uint32_t shift = __builtin_clz(cap - 1);
    uint32_t probe_limit = NBHM_PROBE_MIN_LIMIT + (cap >> 10u);

    int probe = 1;
    uint32_t i = NBHM_FN(hash2index)(table, h);
    for (;;) {
        void* k = nbhm_ldacq(&table->data[i].key);
        void* v = nbhm_ldacq(&table->data[i].val);
        // no entry
        if (k == NULL) { return prev_v; }
        if (NBHM_FN(cmp)(k, key)) {
            if (((uintptr_t) v & EBR_PRIME_BIT) == 0) {
                // since there's a tombstone, that means the value has been
                // written to and then removed in a table *later* than our
                // prev_v which is why we return NULL rather than prev_v.
                return v != NBHM_TOMBSTONE ? v : NULL;
            }
            // found a prime, go search the latest variant
            NBHM_Table* next = nbhm_ldacq(&table->next);
            return NBHM_FN(raw_lookup)(next, h, key, prev_v);
        }
        // same as the put probing, except I don't help writers get stuff done
        // because I'm evil.
        if (++probe >= probe_limit || k == NBHM_TOMBSTONE) {
            NBHM_Table* next = nbhm_ldacq(&table->next);
            return next ? NBHM_FN(raw_lookup)(next, h, key, NULL) : NULL;
        }

        // mask-step-index
        i += (h >> shift) | 1;
        while (i >= cap) { i -= cap; }
    }
}

void* NBHM_FN(get)(NBHM* hm, void* key) {
    EBR__BEGIN("get");

    assert(key);
    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    uint32_t h = NBHM_FN(hash)(key);
    void* v = NBHM_FN(raw_lookup)(curr, h, key, NULL);

    ebr_exit_cs();
    EBR__END();
    return v;
}

void* NBHM_FN(put_if_null)(NBHM* hm, void* key, void* val) {
    EBR__BEGIN("put");

    assert(val);
    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    void* v = NBHM_FN(put_if_match)(curr, key, val, NBHM_TOMBSTONE);
    ebr_exit_cs();
    EBR__END();
    return v;
}

// waits for all items to be moved up before continuing
void NBHM_FN(resize_barrier)(NBHM* hm) {
    EBR__BEGIN("resize_barrier");
    ebr_enter_cs();
    for (;;) {
        NBHM_Table* curr = nbhm_ldacq(&hm->curr);
        NBHM_Table* next = nbhm_ldacq(&curr->next);
        if (next == NULL) { break; }
        NBHM_FN(move_items)(hm, next, curr, curr->cap);
    }
    ebr_exit_cs();
    EBR__END();
}

void NBHM_FN(dump_state)(NBHM* hm) {
    printf("DUMP %d %d %d\n", NBHM_FN(counter_get)(&hm->curr->count), NBHM_FN(counter_get)(&hm->curr->slots), hm->curr->cap);
    for (int i = 0; i < 256; i++) {
        if (histo[i*16]) { printf("%d;%d\n", i, histo[i*16]); }
    }

    for (int i = 0; i < 256; i++) {
        histo[i*16] = 0;
    }
}

#undef NBHM_FN
#endif // NBHM_FN

