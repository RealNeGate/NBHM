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
#include <stdatomic.h>

#include "ebr.h"

enum {
    NBHM_LOAD_FACTOR = 75,
    NBHM_MOVE_AMOUNT = 512,
};

typedef struct {
    _Atomic(void*) key;
    _Atomic(void*) val;
} NBHM_Entry;

typedef struct NBHM_Table NBHM_Table;
struct NBHM_Table {
    _Atomic(NBHM_Table*) next;

    uint32_t cap;

    // reciprocals to compute modulo
    uint64_t a, sh;

    // tracks how many entries have
    // been moved once we're resizing
    _Atomic uint32_t moved;
    _Atomic uint32_t move_done;
    _Atomic uint32_t count;

    NBHM_Entry data[];
};

typedef struct {
    _Atomic(NBHM_Table*) curr;
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
    NBHM_Table* table = EBR_VIRTUAL_ALLOC(sizeof(NBHM_Table) + cap*sizeof(NBHM_Entry));
    nbhm_compute_size(table, cap);
    return (NBHM){ .curr = table };
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
static NBHM_Entry* nbhm_array(NBHM* hs) { return hs->curr->data; }
static size_t nbhm_count(NBHM* hs)      { return hs->curr->count; }
static size_t nbhm_capacity(NBHM* hs)   { return hs->curr->cap; }

#define nbhm_for(it, hs) for (NBHM_Entry *it = nbhm_array(hs), **_end_ = &it[nbhm_capacity(hs)]; it != _end_; it++) if (*it != NULL)
#endif // NBHM_H

#ifdef NBHM_IMPL
#if defined(_WIN32)
#pragma comment(lib, "synchronization.lib")
#endif

int NBHM_TOMBSTONE;
int NBHM_NO_MATCH_OLD;
#endif // NBHM_IMPL

// Templated implementation
#ifdef NBHM_FN
extern int NBHM_TOMBSTONE;
extern int NBHM_NO_MATCH_OLD;

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

static void* NBHM_FN(migrate_item)(NBHM_Table* table, NBHM_Table* new_table, size_t i) {
    // CAS key: NULL -> TOMBSTONE to stop entries from claiming the key
    void* k = atomic_load_explicit(&table->data[i].key, memory_order_relaxed);
    while (k == NULL && !atomic_compare_exchange_strong(&table->data[i].key, &k, &NBHM_TOMBSTONE)) {
        // ...
    }

    if (k == NULL) {
        return NULL;
    }

    // freeze the values by adding a prime bit.
    void* v = atomic_load_explicit(&table->data[i].val, memory_order_relaxed);
    while (((uintptr_t) v & EBR_PRIME_BIT) == 0) {
        uintptr_t primed_v = (v == &NBHM_TOMBSTONE ? 0 : (uintptr_t) v) | EBR_PRIME_BIT;
        if (atomic_compare_exchange_strong(&table->data[i].val, &v, (void*) primed_v)) {
            break;
        }
        // btw, CAS updated v
    }

    // strip prime bit
    v = (void*) ((uintptr_t) v & ~EBR_PRIME_BIT);
    // we can now move the value into the new table
    if (v != NULL) {
        v = NBHM_FN(put_if_match)(new_table, k, v, NULL);
    }

    // TODO(NeGate): we can replace the PRIME entry with a TOMBPRIME now that we've migrated it up.
    // ...

    return v;
}

NBHM_Table* NBHM_FN(move_items)(NBHM* hm, NBHM_Table* top_table, NBHM_Table* old_table, int items_to_move) {
    assert(old_table);
    size_t cap = old_table->cap;

    // snatch up some number of items
    uint32_t old, new;
    do {
        old = atomic_load(&old_table->moved);
        if (old == cap) { return old_table; }
        // cap the number of items to copy... by the cap
        new = old + items_to_move;
        if (new > cap) { new = cap; }
    } while (!atomic_compare_exchange_strong(&old_table->moved, &(uint32_t){ old }, new));

    if (old == new) {
        return old_table;
    }

    EBR__BEGIN("copying old");
    for (size_t i = old; i < new; i++) {
        NBHM_FN(migrate_item)(old_table, top_table, i);
    }
    EBR__END();

    uint32_t done = atomic_fetch_add(&old_table->move_done, new - old);
    done += new - old;

    // Replace the "freshest" known table with the new one, now that we've migrated all entries
    assert(done <= cap);
    if (done == cap && atomic_compare_exchange_strong(&hm->curr, &old_table, top_table)) {
        ebr_free(old_table, sizeof(NBHM_Table) + old_table->cap*sizeof(NBHM_Entry));
        return top_table;
    }

    return old_table;
}

static NBHM_Table* NBHM_FN(resize)(NBHM_Table* table, size_t limit) {
    // make resized table, we'll amortize the moves upward
    size_t new_cap = nbhm_compute_cap(limit*2);

    NBHM_Table* new_top = EBR_VIRTUAL_ALLOC(sizeof(NBHM_Table) + new_cap*sizeof(NBHM_Entry));
    nbhm_compute_size(new_top, new_cap);

    NBHM_Table* exp = NULL;
    if (!atomic_compare_exchange_strong(&table->next, &exp, new_top)) {
        EBR_VIRTUAL_FREE(new_top, sizeof(NBHM_Table) + new_cap*sizeof(NBHM_Entry));
        return exp;
    } else {
        // float s = sizeof(NBHM_Table) + new_cap*sizeof(NBHM_Entry);
        // printf("Resize: %.2f KiB (cap=%zu)\n", s / 1024.0f, new_cap);
        return new_top;
    }
}

static void* NBHM_FN(put_if_match)(NBHM_Table* table, void* key, void* val, void* exp) {
    assert(key);

    void *k, *v;
    for (;;) {
        uint32_t cap = table->cap;
        size_t limit = (cap * NBHM_LOAD_FACTOR) / 100;

        NBHM_Table* next = atomic_load_explicit(&table->next, memory_order_relaxed);
        if (next == NULL && table->count >= limit) {
            next = NBHM_FN(resize)(table, limit);
        }

        // key claiming phase:
        //   once completed we'll have a key inserted into the latest
        //   table (the value might be NULL which means that the entry
        //   is still empty but we've at least agreed where the value
        //   goes).
        bool found = false;
        uint32_t h = NBHM_FN(hash)(key);
        size_t first = NBHM_FN(hash2index)(table, h), i = first;
        do {
            v = atomic_load_explicit(&table->data[i].val, memory_order_acquire);
            k = atomic_load_explicit(&table->data[i].key, memory_order_acquire);

            if (k == NULL) {
                // key was never in the table
                if (val == &NBHM_TOMBSTONE) { return NULL; }

                // fight for empty slot
                if (atomic_compare_exchange_strong(&table->data[i].key, &k, key)) {
                    atomic_fetch_add_explicit(&table->count, 1, memory_order_relaxed);
                    found = true;
                    break;
                }
            }

            if (NBHM_FN(cmp)(k, key)) {
                found = true;
                break;
            }

            // inc & wrap around
            i = (i == cap-1) ? 0 : i + 1;
        } while (i != first);

        // we didn't claim a key, that means the table is entirely full, retry
        // on a bigger table.
        if (next == NULL && !found) {
            next = NBHM_FN(resize)(table, limit);
        }

        // Migration barrier, freeze old entry before inserting to new table
        if (next != NULL) {
            return NBHM_FN(migrate_item)(table, next, i);
        }

        // if the existing value is:
        // * incompatible with the expected value, we don't write.
        // * equal, we don't write (speed opt, one CAS is slower than no CAS).
        if (v != val &&
            // exp is tombstone, we'll only insert if it's empty (and not a migration)
            (exp != &NBHM_TOMBSTONE || (v == NULL || v == &NBHM_TOMBSTONE))
        ) {
            // value writing attempt, if we lose the CAS it means someone could've written a
            // prime (thus the entry was migrated to a later table). It could also mean we lost
            // the insertion fight to another writer and in that case we'll take their value.
            if (atomic_compare_exchange_strong(&table->data[i].val, &v, val)) {
                v = val;
            } else {
                // if we see a prime, the entry has been migrated
                // and we should write to that later table. if not,
                // we simply lost the race to update the value.
                uintptr_t v_raw = (uintptr_t) v;
                if (v_raw & EBR_PRIME_BIT) {
                    continue;
                }
            }
        }

        return v;
    }
}

static NBHM_Table* NBHM_FN(coop_migrate)(NBHM* hm) {
    // Migrate entries into the "next" table, once all are moved we
    // can just replace the current with it.
    NBHM_Table* curr = atomic_load(&hm->curr);
    NBHM_Table* next = atomic_load(&curr->next);
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

    void* v = NBHM_FN(put_if_match)(curr, key, val, &NBHM_NO_MATCH_OLD);
    ebr_exit_cs();
    EBR__END();
    return v;
}

void* NBHM_FN(remove)(NBHM* hm, void* key) {
    EBR__BEGIN("remove");

    assert(key);
    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    void* v = NBHM_FN(put_if_match)(curr, key, &NBHM_TOMBSTONE, &NBHM_NO_MATCH_OLD);
    ebr_exit_cs();
    EBR__END();
    return v;
}

static void* NBHM_FN(raw_lookup)(NBHM_Table* table, uint32_t h, void* key) {
    do {
        size_t cap = table->cap;
        size_t first = NBHM_FN(hash2index)(table, h), i = first;

        do {
            void* v = atomic_load(&table->data[i].val);
            void* k = atomic_load(&table->data[i].key);

            if (k == NULL) {
                // no entry, might be in a newer table
                return NULL;
            } else if (NBHM_FN(cmp)(k, key)) {
                // if we see a non-prime, then it's the latest revision
                if (((uintptr_t) v & EBR_PRIME_BIT) == 0) {
                    return v != &NBHM_TOMBSTONE ? v : NULL;
                }

                // found partial-copy
                break;
            }

            // inc & wrap around
            i = (i == cap-1) ? 0 : i + 1;
        } while (i != first);

        // check if other newer but incomplete tables hold the current answer
        table = atomic_load_explicit(&table->next, memory_order_relaxed);
    } while (table);

    return NULL;
}

void* NBHM_FN(get)(NBHM* hm, void* key) {
    EBR__BEGIN("get");

    assert(key);
    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    uint32_t h = NBHM_FN(hash)(key);
    void* v = NBHM_FN(raw_lookup)(curr, h, key);

    ebr_exit_cs();
    EBR__END();
    return v;
}

void* NBHM_FN(put_if_null)(NBHM* hm, void* key, void* val) {
    EBR__BEGIN("put");

    assert(val);
    ebr_enter_cs();
    NBHM_Table* curr = NBHM_FN(coop_migrate)(hm);

    void* v = NBHM_FN(put_if_match)(curr, key, val, &NBHM_TOMBSTONE);
    ebr_exit_cs();
    EBR__END();
    return v;
}

// waits for all items to be moved up before continuing
void NBHM_FN(resize_barrier)(NBHM* hm) {
    EBR__BEGIN("resize_barrier");
    ebr_enter_cs();
    for (;;) {
        NBHM_Table* curr = atomic_load(&hm->curr);
        NBHM_Table* next = atomic_load(&curr->next);
        if (next == NULL) { break; }
        NBHM_FN(move_items)(hm, next, curr, curr->cap);
    }
    ebr_exit_cs();
    EBR__END();
}

#undef NBHM_FN
#endif // NBHM_FN

