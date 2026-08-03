////////////////////////////////
// NBHM - Non-blocking hashmap
////////////////////////////////
// You wanna do a lot of stuff on a lot of cores? this is for you.
// It's based on the design of Cliff Click's non-blocking hashmap.
//
// You will need to define NBHM_IMPL in one translation unit, similar to other single-header libs:
//
//   #define NBHM_IMPL
//   #include <nbhm.h>
//
// To use it, you'll need to define NBHM_FN and then include the header:
//
//   #define NBHM_FN(n) XXX_hm_ ## n
//   #include <nbhm.h>
//
// This will compile implementations of the table using
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
#define NBHM_API extern "C"
#include <atomic>
#else
#include <stdatomic.h>
#endif

#include "ebr.h"

enum {
    NBHM_PROBE_MIN_LIMIT = 20,

    NBHM_LOAD_FACTOR = 75,
    NBHM_MOVE_AMOUNT = 1024,

    NBHM_CACHE_LINE_SIZE = 64, // in bytes, tweak this when porting in the future
};

typedef struct NBHM_Counter NBHM_Counter;
struct NBHM_Counter {
    NBHM_Counter* next;
    uint64_t length;

    _Atomic uint64_t last_update;
    _Atomic uint64_t last_sum;

    // for the sake of simplicity when porting allocators, I won't ask for
    // 64B cacheline alignment because I technically don't need it. As long
    // as the structure is naturally aligned to 8B, our writes won't cross a
    // cacheline and the distance between the writes means they're gonna be
    // a cacheline apart.
    uint64_t pad[(NBHM_CACHE_LINE_SIZE - sizeof(uint64_t[4])) / 8];

    _Atomic(uint64_t) count[];
};

// The only difference between the shape of the hash-map
// and the hash-set is whether the entries are 2 void*
// or one.
typedef struct NBHM_Table NBHM_Table;
struct NBHM_Table {
    _Atomic(NBHM_Table*) next;

    uint64_t cap;

    // reciprocals to compute modulo
    uint64_t a, sh;

    // tracks how many entries have
    // been moved once we're resizing
    //
    // unfortunately this has to use nasty contention
    // but luckily not for very long.
    alignas(64) _Atomic(uint64_t) moved;
    alignas(64) _Atomic(uint64_t) move_done;

    _Atomic(NBHM_Counter*) slots; // claimed slots
    _Atomic(NBHM_Counter*) count; // non-NULL values

    // KV[i] => [i*2 + 0], [i*2 + 1]
    // K[i]  => [i]
    _Atomic(void*) entries[];
};

typedef struct {
    #ifdef __cplusplus
    NBHM_Table* curr;
    #else
    _Atomic(NBHM_Table*) curr;
    #endif
} NBHM;

typedef struct {
    #ifdef __cplusplus
    NBHM_Table* curr;
    #else
    _Atomic(NBHM_Table*) curr;
    #endif
} NBHS;

typedef struct {
    // cached table & KV index
    NBHM_Table* table;
    uint64_t limit;
    uint64_t i;

    // current snapshot, you gotta use nbhm_tx_val to
    // unpack what "v" holds since there's special states.
    void* k;
    void* v;
} NBHM_Tx;

#define NBHM_TOMBSTONE ((void*) 1)

// converts the in-memory value slot into something
// useful, either treating tombstones as NULL or
// unpriming the value.
static void* nbhm_tx_val(NBHM_Tx* tx) {
    uintptr_t v = (uintptr_t) tx->v;
    if (tx->v == NBHM_TOMBSTONE) { return NULL; }
    if (v & EBR_PRIME_BIT) { return (void*) (v & ~EBR_PRIME_BIT); }
    return tx->v;
}

NBHM nbhm_alloc(size_t initial_cap);
NBHS nbhs_alloc(size_t initial_cap);

void nbhm_free(NBHM* table);
void nbhs_free(NBHS* table);

typedef struct {
    NBHM_Table* table;
    size_t i;
    void *k, *v;
} NBHM_Iter;

// Iterators:
//   WARNING WARNING WARNING you can't call this until you've stopped
//   updating the table and done a resize barrier, if there's enough
//   demand I could make a snapshot mechanism or something.
#define NBHM_FOR(it, table) for (NBHM_Iter it = nbhm_iter(table); nbhm_iter_next(&(it));)
#define NBHS_FOR(it, table) for (NBHM_Iter it = nbhs_iter(table); nbhs_iter_next(&(it));)

NBHM_Iter nbhm_iter(NBHM* table);
NBHM_Iter nbhs_iter(NBHS* table);
bool nbhm_iter_next(NBHM_Iter* iter);
bool nbhs_iter_next(NBHM_Iter* iter);
size_t nbhm_UNSAFE_count(NBHM* table);
size_t nbhs_UNSAFE_count(NBHS* table);

#endif // NBHM_H

// Shared code between both the NBHM_IMPL and NBHM_FN
#if defined(NBHM_IMPL) || defined(NBHM_FN)
#include "model_check.h"

#ifdef MC_TESTING
#define nbhm_ldrlx(addr)         (MC_SEQ("LD"),  atomic_load_explicit(addr, memory_order_relaxed))
#define nbhm_ldacq(addr)         (MC_SEQ("LD"),  atomic_load_explicit(addr, memory_order_acquire))
#define nbhm_strel(addr, x)      (MC_SEQ("ST"),  atomic_store_explicit(addr, x, memory_order_release))
#define nbhm_cas_strong(a, b, c) (MC_SEQ("CAS"), atomic_compare_exchange_strong_explicit(a, b, c, memory_order_acq_rel, memory_order_acquire))
#define nbhm_cas_weak(a, b, c)   (MC_SEQ("CAS"), atomic_compare_exchange_weak_explicit(a, b, c, memory_order_acq_rel, memory_order_acquire))
#define nbhm_fetch_add(a, b)     (MC_SEQ("FAA"), atomic_fetch_add_explicit(a, b, memory_order_acq_rel))
#else
#define nbhm_ldrlx(addr)         atomic_load_explicit(addr, memory_order_relaxed)
#define nbhm_ldacq(addr)         atomic_load_explicit(addr, memory_order_acquire)
#define nbhm_strel(addr, x)      atomic_store_explicit(addr, x, memory_order_release)
#define nbhm_cas_strong(a, b, c) atomic_compare_exchange_strong_explicit(a, b, c, memory_order_acq_rel, memory_order_acquire)
#define nbhm_cas_weak(a, b, c)   atomic_compare_exchange_weak_explicit(a, b, c, memory_order_acq_rel, memory_order_acquire)
#define nbhm_fetch_add(a, b)     atomic_fetch_add_explicit(a, b, memory_order_acq_rel)
#endif

void nbhm__counter_add(_Atomic(NBHM_Counter*)* dst, int delta);
void nbhm__counter_free(NBHM_Counter* cnt);
uint64_t nbhm__counter_get(NBHM_Counter* cnt);
uint64_t nbhm__counter_estimate(NBHM_Counter* cnt);
#endif

#ifdef NBHM_IMPL
// (X + Y) / Z = int(X/Z) + int(Y/Z) + (mod(X,Z) + mod(Y,Z)/Z
uint64_t negate__div128(uint64_t numhi, uint64_t numlo, uint64_t den, uint64_t* out_rem) {
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

void nbhm__compute_size(NBHM_Table* table, size_t cap) {
    uint64_t sh = 0;
    while (cap > (1ull << sh)){ sh++; }

    #if defined(__GNUC__) || defined(__clang__)
    table->sh = 63 - __builtin_clzll(cap);
    #else
    table->sh = sh - 1;
    #endif

    assert(sh - 1 == table->sh);
    table->a = negate__div128(1ull << table->sh, cap - 1, cap, NULL);

    // reciprocal to compute modulo
    #if (defined(__GNUC__) || defined(__clang__)) && defined(__x86_64__)
    uint64_t d,e;
    __asm__("div %[v]" : "=a"(d), "=d"(e) : [v] "r"(cap), "a"(cap - 1), "d"(1ull << table->sh));
    assert(d == table->a);
    #endif
    table->cap = cap;

    size_t counter_size = sizeof(NBHM_Counter) + 8*sizeof(uint64_t);
    table->slots = EBR_REALLOC(NULL, counter_size);
    memset(table->slots, 0, counter_size);
    table->slots->length = 8;

    if (table->count == NULL) {
        table->count = EBR_REALLOC(NULL, counter_size);
        memset(table->count, 0, counter_size);
        table->count->length = 8;
    }
}

size_t nbhm__compute_cap(size_t y, size_t entry_size) {
    // minimum capacity
    if (y < 256) {
        y = 256;
    } else {
        y = ((y + 1) / 3) * 4;
    }

    size_t cap = 1ull << (64 - __builtin_clzll(y - 1));
    return cap - (sizeof(NBHM_Table) / entry_size);
}

static NBHM_Table* nbhm__alloc_internal(size_t initial_cap, size_t entry_size) {
    ebr_init();

    size_t cap = nbhm__compute_cap(initial_cap, entry_size);
    NBHM_Table* table = (NBHM_Table*) EBR_VIRTUAL_ALLOC(sizeof(NBHM_Table) + cap*entry_size);
    nbhm__compute_size(table, cap);
    return table;
}

static void nbhm__free(NBHM_Table* curr, size_t entry_size) {
    while (curr) {
        NBHM_Table* next = curr->next;
        EBR_VIRTUAL_FREE(curr, sizeof(NBHM_Table) + curr->cap*entry_size);
        curr = next;
    }
}

NBHS nbhs_alloc(size_t initial_cap) {
    return (NBHS){ nbhm__alloc_internal(initial_cap, sizeof(void*)) };
}

NBHM nbhm_alloc(size_t initial_cap) {
    return (NBHM){ nbhm__alloc_internal(initial_cap, sizeof(void*[2])) };
}

size_t nbhs_UNSAFE_count(NBHS* table) {
    NBHM_Table* curr = nbhm_ldacq(&table->curr);
    return nbhm__counter_get(curr->count);
}

size_t nbhm_UNSAFE_count(NBHM* table) {
    NBHM_Table* curr = nbhm_ldacq(&table->curr);
    return nbhm__counter_get(curr->count);
}

void nbhs_free(NBHS* table) { nbhm__free(table->curr, sizeof(void*)); }
void nbhm_free(NBHM* table) { nbhm__free(table->curr, sizeof(void*[2])); }

NBHM_Iter nbhs_iter(NBHS* table) {
    return (NBHM_Iter){ nbhm_ldacq(&table->curr), 0, 0, 0 };
}

NBHM_Iter nbhm_iter(NBHM* table) {
    return (NBHM_Iter){ nbhm_ldacq(&table->curr), 0, 0, 0 };
}

bool nbhs_iter_next(NBHM_Iter* iter) {
    NBHM_Table* table = iter->table;
    size_t i = iter->i;
    for (; i < table->cap; i++) {
        void* k = nbhm_ldacq(&table->entries[i]);
        // skip empty keys
        if (k == NULL || k == NBHM_TOMBSTONE) {
            continue;
        }
        iter->k = k;
        iter->v = NULL;
        iter->i = i + 1;
        return true;
    }
    return false;
}

bool nbhm_iter_next(NBHM_Iter* iter) {
    NBHM_Table* table = iter->table;
    size_t i = iter->i;
    for (; i < table->cap; i++) {
        void* k = nbhm_ldacq(&table->entries[i*2]);
        void* v = nbhm_ldacq(&table->entries[i*2 + 1]);
        // skip empty keys or vals
        if (k == NULL || k == NBHM_TOMBSTONE ||
            v == NULL || v == NBHM_TOMBSTONE) {
            continue;
        }
        iter->k = k;
        iter->v = v;
        iter->i = i + 1;
        return true;
    }
    return false;
}

size_t nbhm__hash2index(NBHM_Table* table, uint64_t u) {
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
    uint64_t q  = hi >> table->sh;
    assert(q == u / table->cap);

    // Modulo from quotient
    uint64_t q2 = u - (q * table->cap);
    assert(q2 == u % table->cap);
    return q2;
}

////////////////////////////////
// Sharded Counters
////////////////////////////////
void nbhm__counter_add(_Atomic(NBHM_Counter*)* dst, int delta) {
    if (delta) {
        // uint64_t start = __rdtsc();

        NBHM_Counter* cnt = nbhm_ldacq(dst);
        uint32_t hash = (ebr_thread_id * 8) & (cnt->length - 1);

        #if 0
        nbhm_fetch_add(&cnt->count[hash], delta);
        #else
        assert(hash < cnt->length);
        uint64_t curr = nbhm_ldacq(&cnt->count[hash]);
        if (nbhm_cas_strong(&cnt->count[hash], &curr, curr + delta)) {
            return;
        }

        // Resize since there was at least one CAS fail due to contention
        nbhm_fetch_add(&cnt->count[hash], delta);
        if (cnt->length >= 4096 || nbhm_ldacq(dst) != cnt) {
            return;
        }

        size_t counter_size = sizeof(NBHM_Counter) + (cnt->length * 2)*sizeof(uint64_t);
        NBHM_Counter* new_cnt = EBR_REALLOC(NULL, counter_size);
        memset(new_cnt, 0, counter_size);
        new_cnt->length = cnt->length * 2;
        new_cnt->next = cnt;

        // Only way this fails is if someone else resized
        if (!nbhm_cas_strong(dst, &cnt, new_cnt)) {
            (void) EBR_REALLOC(new_cnt, 0);
        } else {
            // printf("Resize counter!!! %zx %zu\n", new_cnt->length, new_cnt->length / 8);
        }
        #endif

        // uint64_t delta = __rdtsc() - start;
        // histo[(delta > 255 ? 255 : delta) * 16] += 1;
    }
}

void nbhm__counter_free(NBHM_Counter* cnt) {
    do {
        ebr_free(cnt, sizeof(NBHM_Counter) + cnt->length*sizeof(uint64_t), true);
        cnt = cnt->next;
    } while (cnt);
}

uint64_t nbhm__counter_get(NBHM_Counter* cnt) {
    uint64_t total = 0;
    do {
        for (int i = 0; i < cnt->length; i += 8) {
            total += nbhm_ldacq(&cnt->count[i]);
        }
        cnt = cnt->next;
    } while (cnt);
    return total;
}

uint64_t nbhm__counter_estimate(NBHM_Counter* cnt) {
    // uint64_t start = __rdtsc();

    #if 0
    uint64_t s = nbhm__counter_get(cnt);
    #else
    // only update the estimate every millisecond
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    uint64_t ticks = (ts.tv_sec * 1000L) + (ts.tv_nsec / 1000L);
    if (nbhm_ldacq(&cnt->last_update) != ticks) {
        nbhm_strel(&cnt->last_sum,    nbhm__counter_get(cnt));
        nbhm_strel(&cnt->last_update, ticks);
    }
    uint64_t s = nbhm_ldacq(&cnt->last_sum);
    #endif

    // uint64_t delta = (__rdtsc() - start) / 10;
    // histo[(delta > 255 ? 255 : delta) * 16] += 1;
    return s;
}

#endif // NBHM_IMPL

// Templated implementation
#ifdef NBHM_FN
#include <x86intrin.h>

size_t nbhm__hash2index(NBHM_Table* table, uint64_t u);
size_t nbhm__compute_cap(size_t y, size_t entry_size);
void nbhm__compute_size(NBHM_Table* table, size_t cap);

#ifdef NBHM_IS_SET
#define NBHM_T NBHS
#define NBHM_K(table, i) &(table)->entries[i]
#else
#define NBHM_T NBHM
#define NBHM_K(table, i) &(table)->entries[(i)*2 + 0]
#define NBHM_V(table, i) &(table)->entries[(i)*2 + 1]
#endif

// static _Atomic int histo[256*16];

static NBHM_Tx NBHM_FN(tx_begin)(NBHM_Table* table, void* key, bool abort_if_null);

#ifndef NBHM_IS_SET // NBHM only code
static bool NBHM_FN(tx_commit)(NBHM_Tx* tx, void* val, bool migrate);
#endif

static void NBHM_FN(migrate_item)(NBHM_Table* table, NBHM_Table* new_table, size_t i) {
    // CAS key: NULL -> TOMBSTONE to stop entries from claiming the key.
    void* k = nbhm_ldacq(NBHM_K(table, i));
    if (k == NULL && nbhm_cas_strong(NBHM_K(table, i), &k, NBHM_TOMBSTONE)) {
        return;
    }

    // we already caught the case where a key claim won, now we just need to check
    // that another migrate_item didn't win
    if (k == NBHM_TOMBSTONE) {
        return;
    }

    #ifdef NBHM_IS_SET
    // Hashset doesn't allow removals so it doesn't need to CAS in the primes.
    // We just need to fence so that the key in the old table is inserted before
    // any further ops can be performed.
    NBHM_FN(tx_begin)(new_table, k, false);
    #else
    // freeze the values by adding a prime bit.
    void* old_v = nbhm_ldacq(NBHM_V(table, i));
    while (((uintptr_t) old_v & EBR_PRIME_BIT) == 0) {
        uintptr_t primed_v = (old_v == NBHM_TOMBSTONE ? 0 : (uintptr_t) old_v) | EBR_PRIME_BIT;
        if (nbhm_cas_weak(NBHM_V(table, i), &old_v, (void*) primed_v)) {
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

    // insert if NULL
    NBHM_Tx tx = NBHM_FN(tx_begin)(new_table, k, false);
    while (tx.v == NULL && !NBHM_FN(tx_commit)(&tx, v, true));

    // TODO(NeGate): we can replace the PRIME entry with a TOMBPRIME now that we've migrated it up.
    // ...
    #endif
}

NBHM_Table* NBHM_FN(move_items)(NBHM_T* hm, NBHM_Table* top_table, NBHM_Table* old_table, size_t items_to_move) {
    assert(old_table);
    size_t cap = old_table->cap;

    // snatch up some number of items
    uint64_t new;
    uint64_t old = nbhm_ldacq(&old_table->moved);
    do {
        if (old == cap) { return old_table; }
        // cap the number of items to copy... by the cap
        new = old + items_to_move;
        if (new > cap) { new = cap; }
    } while (!nbhm_cas_weak(&old_table->moved, &old, new));

    if (old == new) {
        return old_table;
    }

    EBR__BEGIN("copying old");
    for (size_t i = old; i < new; i++) {
        NBHM_FN(migrate_item)(old_table, top_table, i);
    }
    EBR__END();

    uint64_t done = nbhm_fetch_add(&old_table->move_done, new - old);
    done += new - old;

    // We can think of the tables as a queue, now that we've migrated all entries
    // from the head to the next node, we can advance the head and later on let EBR
    // free that entry.
    assert(done <= cap);
    if (done == cap && nbhm_cas_strong(&hm->curr, &old_table, top_table)) {
        #ifdef NBHM_IS_SET
        size_t entry_size = sizeof(void*);
        #else
        size_t entry_size = sizeof(void*[2]);
        #endif

        // nbhm__counter_free(old_table->slots);
        // nbhm__counter_free(old_table->count);
        ebr_free(old_table, sizeof(NBHM_Table) + old_table->cap*entry_size, false);
        return top_table;
    }

    return old_table;
}

static NBHM_Table* NBHM_FN(resize)(NBHM_Table* table, size_t limit) {
    NBHM_Table* next = nbhm_ldacq(&table->next);
    if (next != NULL) {
        return next;
    }

    // TODO(NeGate): we could implement a simple contention pacing
    // mechanism here, have all the resizers at this time fight for a
    // "bias" and then make the losers wait a bit before giving it a shot.
    // ...

    #ifdef NBHM_IS_SET
    size_t entry_size = sizeof(void*);
    #else
    size_t entry_size = sizeof(void*[2]);
    #endif

    // Only grow the size if we're actually needing more slots, sometimes
    // we just have a lot of tombstones from earlier key claims which have
    // gone unused.
    size_t new_cap = table->cap;
    if (nbhm__counter_get(table->count) >= new_cap / 2) {
        new_cap = nbhm__compute_cap(limit * 3, entry_size);
    }

    // make resized table, we'll amortize the moves upward.
    NBHM_Table* new_top = EBR_VIRTUAL_ALLOC(sizeof(NBHM_Table) + new_cap*entry_size);
    new_top->count = table->count;
    nbhm__compute_size(new_top, new_cap);

    NBHM_Table* exp = NULL;
    if (!nbhm_cas_strong(&table->next, &exp, new_top)) {
        EBR_VIRTUAL_FREE(new_top, sizeof(NBHM_Table) + new_cap*entry_size);
        return exp;
    } else {
        // float s = sizeof(NBHM_Table) + new_cap*entry_size;
        // printf("Resize: %p %.2f KiB (cap=%zu, %lu %lu)\n", new_top, s / 1024.0f, new_cap, nbhm__counter_get(table->count), nbhm__counter_get(table->slots));
        return new_top;
    }
}

// if abort_if_null is true, we'll end the transaction prematurely in case
// there's an empty key slot (this is used for HM removal). In all other cases
// the transaction shall produce a non-NULL key.
static NBHM_Tx NBHM_FN(tx_begin)(NBHM_Table* table, void* key, bool abort_if_null) {
    NBHM_Tx tx = { 0 };
    uint32_t h = NBHM_FN(hash)(key);

    void *k, *v;
    uint32_t cap   = table->cap;
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
    size_t i = nbhm__hash2index(table, h);
    for (;;) {
        k = nbhm_ldacq(NBHM_K(table, i));

        #ifdef NBHM_IS_SET
        v = NULL;
        #else
        v = nbhm_ldacq(NBHM_V(table, i));
        #endif

        if (k == NULL) {
            // key was never in the table
            if (abort_if_null) {
                return tx;
            }

            // fight for empty slot
            if (nbhm_cas_strong(NBHM_K(table, i), &k, key)) {
                nbhm__counter_add(&table->slots, 1);
                found = true;
                k = key;

                #ifdef NBHM_IS_SET
                v = key;
                #endif
                break;
            }
        }

        if (k != NBHM_TOMBSTONE && (k == key || NBHM_FN(cmp)(k, key))) {
            found = true;
            break;
        }

        // if we reprobe enough, just make a new table.
        // if we see a tombstone then the table is currently mid-migration
        // so we might as well use the new one.
        if (++probe >= probe_limit || k == NBHM_TOMBSTONE) {
            NBHM_Table* next = NBHM_FN(resize)(table, limit);
            return NBHM_FN(tx_begin)(next, key, abort_if_null);
        }

        // mask-step-index
        i += (h >> shift) | 1;
        while (i >= cap) { i -= cap; }
    }

    tx.table = table;
    tx.limit = limit;
    tx.i = i;
    tx.k = k;
    tx.v = v;
    return tx;
}

#ifndef NBHM_IS_SET // NBHM only code
// returns true if our atomic transaction from tx->v => val succeeds.
//
// Function is allowed to spuriously failed to match weak CAS semantics (allowing
// for some really neat tricks, at least for me).
static bool NBHM_FN(tx_commit)(NBHM_Tx* tx, void* val, bool migrate) {
    NBHM_Table* table = tx->table;
    size_t i = tx->i;
    void* k  = tx->k;
    void* v  = tx->v;

    // This would imply that we didn't claim a slot, maybe tx_begin
    // was told to abort on NULL but the caller still called commit?
    assert(k != NULL);

    // if we're about to insert into a stuffed table, maybe don't? and if
    // we see a prime that's our queue to retry the transaction on the freshest
    // table.
    if ((v == NULL && nbhm__counter_estimate(table->count) >= tx->limit) ||
        ((uintptr_t) v & EBR_PRIME_BIT)) {
        NBHM_Table* next = NBHM_FN(resize)(table, tx->limit);
        NBHM_FN(migrate_item)(table, next, i);

        // redo the transaction begin on the new table
        *tx = NBHM_FN(tx_begin)(next, k, val == NULL);
        return false;
    }

    // value writing attempt, if we lose the CAS it means someone could've written
    // a prime (thus the entry was migrated to a later table). It could also mean
    // we lost the insertion fight to another writer and in that case we'll take
    // their value.
    if (v != val && !nbhm_cas_weak(NBHM_V(table, i), &v, val)) {
        // update to the value of the winner
        tx->v = v;
        return false;
    }

    // migrations don't report the old value of this slot, they report the
    // value of the earliest slot.
    void* old_val = migrate ? val : v;
    (void) MC_REPORT_READ(k,  old_val);
    MC_REPORT_WRITE(k, val);

    // tally up how many values are in the same (so not including claimed
    // slots with tombstones)
    if (v == NULL || v == NBHM_TOMBSTONE) {
        if (val != NBHM_TOMBSTONE) { nbhm__counter_add(&table->count,  1); }
    } else {
        if (val == NBHM_TOMBSTONE) { nbhm__counter_add(&table->count, -1); }
    }
    return true;
}
#endif

// for a hash-set this returns the key, for a hash-map it returns the value.
static void* NBHM_FN(raw_lookup)(NBHM_Table* table, uint32_t h, void* key, void* prev_v) {
    uint32_t cap   = table->cap;
    uint32_t shift = __builtin_clz(cap - 1);
    uint32_t probe_limit = NBHM_PROBE_MIN_LIMIT + (cap >> 10u);

    int probe  = 1;
    uint32_t i = nbhm__hash2index(table, h);
    for (;;) {
        void* k = nbhm_ldacq(NBHM_K(table, i));
        #ifndef NBHM_IS_SET
        void* v = nbhm_ldacq(NBHM_V(table, i));
        #endif
        // no entry
        if (k == NULL) {
            (void) MC_REPORT_READ(key, prev_v);
            return (void*) ((uintptr_t) prev_v & ~EBR_PRIME_BIT);
        }
        if (k != NBHM_TOMBSTONE && (k == key || NBHM_FN(cmp)(k, key))) {
            #ifdef NBHM_IS_SET
            return k;
            #else
            if (((uintptr_t) v & EBR_PRIME_BIT) == 0) {
                // since there's a tombstone, that means the value has been
                // written to and then removed in a table *later* than our
                // prev_v which is why we return NULL rather than prev_v.
                (void) MC_REPORT_READ(k, v);
                return v != NBHM_TOMBSTONE ? v : NULL;
            }
            // found a prime, go search the latest variant. if there's none
            // then we'll default to this v because it's the "latest" one
            // we saw.
            NBHM_Table* next = nbhm_ldacq(&table->next);
            return NBHM_FN(raw_lookup)(next, h, key, v);
            #endif
        }
        // same as the put probing, except I don't help writers get stuff done
        // because I'm evil.
        if (++probe >= probe_limit || k == NBHM_TOMBSTONE) {
            NBHM_Table* next = nbhm_ldacq(&table->next);
            return next ? NBHM_FN(raw_lookup)(next, h, key, NULL) : MC_REPORT_READ(key, NULL);
        }
        // mask-step-index
        i += (h >> shift) | 1;
        while (i >= cap) { i -= cap; }
    }
}

static NBHM_Table* NBHM_FN(coop_migrate)(NBHM_T* hm) {
    // Migrate entries into the "next" table, once all are moved we
    // can just replace the current with it.
    NBHM_Table* curr = nbhm_ldacq(&hm->curr);
    NBHM_Table* next = nbhm_ldacq(&curr->next);
    if (next != NULL) {
        return NBHM_FN(move_items)(hm, next, curr, NBHM_MOVE_AMOUNT);
    }
    return curr;
}

#ifdef NBHM_IS_SET
void* NBHM_FN(intern)(NBHM_T* hm, void* key) {
    EBR__BEGIN("intern");
    MC_REPORT_BEGIN();

    assert(key && key != NBHM_TOMBSTONE);

    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);
    NBHM_Tx tx = NBHM_FN(tx_begin)(curr, key, false);
    if (tx.v != NULL) {
        nbhm__counter_add(&curr->count, 1);
    }
    ebr_exit_cs();

    EBR__END();
    return tx.v ? key : tx.k;
}
#else
void* NBHM_FN(put)(NBHM_T* hm, void* key, void* val) {
    EBR__BEGIN("put");
    MC_REPORT_BEGIN();

    assert(key && key != NBHM_TOMBSTONE);
    assert(val);

    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    // just insert, no care for the old value
    NBHM_Tx tx = NBHM_FN(tx_begin)(curr, key, false);
    while (!NBHM_FN(tx_commit)(&tx, val, false));

    ebr_exit_cs();
    EBR__END();
    return nbhm_tx_val(&tx);
}

void* NBHM_FN(remove)(NBHM_T* hm, void* key) {
    EBR__BEGIN("remove");
    MC_REPORT_BEGIN();

    assert(key && key != NBHM_TOMBSTONE);

    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    // replace value with tombstone if not already empty
    NBHM_Tx tx = NBHM_FN(tx_begin)(curr, key, true);
    while (nbhm_tx_val(&tx) != NULL && !NBHM_FN(tx_commit)(&tx, NBHM_TOMBSTONE, false));

    ebr_exit_cs();
    EBR__END();
    return nbhm_tx_val(&tx);
}

void* NBHM_FN(put_if_null)(NBHM_T* hm, void* key, void* val) {
    EBR__BEGIN("put");
    MC_REPORT_BEGIN();

    assert(key && key != NBHM_TOMBSTONE);
    assert(val);

    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    NBHM_Tx tx = NBHM_FN(tx_begin)(curr, key, false);
    while (nbhm_tx_val(&tx) == NULL && !NBHM_FN(tx_commit)(&tx, val, false));

    ebr_exit_cs();
    EBR__END();
    return nbhm_tx_val(&tx);
}
#endif

void* NBHM_FN(get)(NBHM_T* hm, void* key) {
    EBR__BEGIN("get");
    MC_REPORT_BEGIN();
    assert(key && key != NBHM_TOMBSTONE);

    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    uint32_t h = NBHM_FN(hash)(key);
    void* v = NBHM_FN(raw_lookup)(curr, h, key, NULL);

    ebr_exit_cs();
    EBR__END();
    return v;
}

// waits for all items to be moved up before continuing
void NBHM_FN(resize_barrier)(NBHM_T* hm) {
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

// internal macros
#undef NBHM_K
#undef NBHM_V

#undef NBHM_IS_SET
#undef NBHM_FN
#endif // NBHM_FN

