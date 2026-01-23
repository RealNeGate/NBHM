#define VECTORS 0
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/stat.h>
#define USE_SPALL 0

#if USE_SPALL
#define SPALL_AUTO_IMPLEMENTATION
#include "spall_native_auto.h"
#else
#define spall_auto_buffer_begin(...)
#define spall_auto_buffer_end(...)
#endif

#if VECTORS
#include <x86intrin.h>
#endif

#ifdef _WIN32
#define fileno _fileno
#define fstat _fstat64
#define stat _stat64
#endif

typedef enum {
    COL_SOURCE,    // Source
    COL_BS,        // B/S
    COL_ORDER_QTY, // OrdQty
    COL_EXEC_QTY,  // ExcQty
    COL_WORK_QTY,  // WrkQty
    COL_PROD,      // Prod

    COL_MAX,
} Column;

typedef enum { FROM_HOST, FROM_CLIENT, TO_HOST, TO_CLIENT } Source;
typedef enum { BUY, SELL } BS;

static const char* col_names[] = {
    "Source", "B/S", "OrdQty", "ExcQty", "WorkQty", "Prod"
};

typedef _Atomic(int64_t) atomic_int64_t;

typedef struct {
    atomic_int64_t total;
    atomic_int entries;
    atomic_int buys;
    atomic_int sells;
} Product;

typedef struct {
    uint32_t key;
    Product  val;
} ProductEntry;

static uint32_t my_hash(const void* a) {
    uint32_t x = ((const ProductEntry*) a)->key;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = (x >> 16) ^ x;
    return x;
}

static bool my_cmp(const void* a, const void* b) {
    uint32_t x = ((const ProductEntry*) a)->key;
    uint32_t y = ((const ProductEntry*) b)->key;
    return x == y;
}

#define EBR_IMPL
#include "../ebr.h"

#define NBHS_IMPL
#define NBHS_FN(n) my_ ## n
#include "../nbhs.h"

static NBHS product_table;
static void ph_update(uint32_t key, Product p) {
    // Switch in some simple arena here
    ProductEntry* prod = malloc(sizeof(ProductEntry));
    prod->key = key;
    prod->val = p;

    ProductEntry* k = my_intern(&product_table, prod);
    if (k != prod) {
        free(prod);

        // Update entries
        k->val.entries += 1;
        k->val.total   += p.total;
        k->val.buys    += p.buys;
        k->val.sells   += p.sells;
    }
}

enum { LINE_BUFFER_CAP = 2*1024*1024 };
static thread_local char* line_buffer;

// You ready for some bullshit?
// Skipping 8 characters at a time, searching for newlines!
static const uint64_t nl_mask = (~(uint64_t)0) / 255 * (uint64_t)('\n');
#define has_zero(x) (((x)-(uint64_t)(0x0101010101010101)) & ~(x)&(uint64_t)(0x8080808080808080))

static size_t skip_to_end(const char* str, size_t len, size_t curr) {
    #if !VECTORS
    uint64_t chunk;
    while (curr + 8 < len) {
        memcpy(&chunk, str + curr, sizeof(chunk));
        uint64_t xor = chunk ^ nl_mask;
        if (!has_zero(xor)) {
            curr += 8;
        } else {
            break;
        }
    }
    #else
    while (curr + 16 < len) {
        __m128i chars = _mm_loadu_si128((__m128i*) &line_buffer[curr]);
        __m128i mask = _mm_cmpeq_epi8(chars, _mm_set1_epi8('\n'));
        uint32_t count = __builtin_ffs(_mm_movemask_epi8(mask));
        if (count != 0) {
            curr += count - 1;
            break;
        }

        // good path, we ballin
        curr += 16;
    }
    #endif

    while (curr < len) {
        char ch = str[curr];
        if (ch == '\n') {
            break;
        }
        curr++;
    }

    return curr;
}

static size_t skip_to_next(const char* str, size_t len, size_t i, size_t times, size_t curr) {
    while (curr < len && i < times) {
        char ch = str[curr++];
        if (ch == ',' || ch == '\n') {
            i++;
        }
    }

    return curr;
}

// need to guarentee that it's safe to read 8bytes, the line buffer pads a bit just in case
static uint64_t as_bits(size_t len, const char* str) {
    if (len > 8) {
        printf("ERROR: '%.*s'\n", (int)len, str);
    }
    assert(len <= 8);
    uint64_t name;
    memcpy(&name, str, 8);
    return name & (UINT64_MAX >> (64 - (len * 8)));
}

static int intcol(int target, size_t n, int* col_ptr, size_t* index) {
    int col = *col_ptr;
    assert(col <= target);

    size_t i = *index;
    size_t last_comma = i;
    while (col <= target) {
        last_comma = i;
        while (line_buffer[i] != ',' && line_buffer[i] != '\n') i++;
        i += 1, col += 1;
    }

    size_t col_len = i - last_comma - 1;

    int p = 0;
    for (ptrdiff_t j = 0; j < col_len; j++) {
        p = (p * 10) + (line_buffer[last_comma+j] - '0');
    }

    // write-back
    *col_ptr = col;
    *index = i;
    return p;
}

static uint64_t strcol(int target, size_t n, int* col_ptr, size_t* index) {
    int col = *col_ptr;
    assert(col <= target);

    size_t i = *index;
    size_t last_comma = i;
    while (col <= target) {
        last_comma = i;
        while (line_buffer[i] != ',' && line_buffer[i] != '\n') i++;
        i += 1, col += 1;
    }

    done:;
    size_t col_len = i - last_comma - 1;

    // write-back
    *col_ptr = col;
    *index = i;
    return as_bits(col_len, &line_buffer[last_comma]);
}

static _Atomic uint64_t total_time;
static uint64_t get_nanos(void) {
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static int process(void* arg) {
    const char* path = arg;

    #if USE_SPALL
    static atomic_int tid;
    spall_auto_thread_init(++tid, SPALL_DEFAULT_BUFFER_SIZE);
    #endif

    uint64_t start_time = get_nanos();
    line_buffer = malloc(LINE_BUFFER_CAP + 16);

    FILE* fp = fopen(path, "rb");
    size_t n = fread(line_buffer, 1, LINE_BUFFER_CAP, fp);
    line_buffer[n] = '\n';

    // read initial line
    EBR__BEGIN("parse schema");
    int col = 0;
    size_t i = 0, last_comma = 0;
    int bs_idx, prd_idx, exc_idx, src_idx, ord_idx, wrk_idx;
    while (i < n) {
        char ch = line_buffer[i++];
        if (ch == ',' || ch == '\n') {
            size_t col_len = i - last_comma - 1;

            // write if we see a relevant column, mark it
            const char* str = &line_buffer[last_comma];
            switch (col_len) {
                case 3: if (!memcmp(str, "B/S",    3)) { bs_idx = col;  } break;
                case 4: if (!memcmp(str, "Prod",   4)) { prd_idx = col; } break;
                case 6: if (!memcmp(str, "ExcQty", 6)) { exc_idx = col; }
                else    if (!memcmp(str, "Source", 6)) { src_idx = col; }
                else    if (!memcmp(str, "OrdQty", 6)) { ord_idx = col; }
                else    if (!memcmp(str, "WrkQty", 6)) { wrk_idx = col; } break;
            }
            last_comma = i;
            col++;

            if (ch == '\n') {
                break;
            }
        }
    }
    EBR__END();

    EBR__BEGIN("parse");
    uint64_t to_client = as_bits(6, "ToClnt\0\0");
    uint64_t buy = as_bits(3, "Buy\0\0\0\0\0");

    for (;;) {
        // make sure the entire line is in the chunk
        size_t eol = skip_to_end(line_buffer, n, i);
        if (eol == n) {
            EBR__BEGIN("read");
            // shift partial line to the start
            size_t j = LINE_BUFFER_CAP - i;
            memmove(line_buffer, line_buffer + i, j);
            n = fread(line_buffer+j, 1, i, fp);
            if (n <= 0) { EBR__END(); goto end; }
            line_buffer[j+n] = '\n';

            i = 0;
            n += j;
            eol = skip_to_end(line_buffer, n, j);
            EBR__END();
        }

        col = 0, last_comma = 0;

        // filter based on Source
        if (strcol(src_idx, n, &col, &i) == to_client) {
            EBR__BEGIN("entry");
            uint64_t bs   = strcol(bs_idx,  n, &col, &i);
            int ord       = intcol(ord_idx, n, &col, &i);
            int wrk       = intcol(wrk_idx, n, &col, &i);
            int exc       = intcol(exc_idx, n, &col, &i);
            uint64_t prod = strcol(prd_idx, n, &col, &i);

            // > For each unique product, count Buys vs Sells. Take the Max of
            // > the 3 Qty columns, and total it, along with counting the number
            // > of entries.
            int m = ord;
            if (m < wrk) m = wrk;
            if (m < exc) m = exc;

            Product p = { .entries = 1, .total = m, .buys = bs == buy, .sells = bs != buy };
            ph_update(prod, p);
            EBR__END();
        }

        i = eol + 1;
    }

    end:
    EBR__END();
    fclose(fp);

    #if USE_SPALL
    spall_auto_thread_quit();
    #endif

    return 0;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        printf("Invalid args for %s\n", argv[0]);
        return 1;
    }

    #if USE_SPALL
    spall_auto_init((char *)"profile.spall");
    spall_auto_thread_init(0, SPALL_DEFAULT_BUFFER_SIZE);
    #endif

    product_table = nbhs_alloc(32);

    int num_threads = 4;
    if (1) {
        EBR__BEGIN("process");
        thrd_t* arr = malloc(num_threads * sizeof(thrd_t));
        for (int i = 0; i < num_threads; i++) {
            thrd_create(&arr[i], process, argv[1]);
        }
        for (int i = 0; i < num_threads; i++) {
            thrd_join(arr[i], NULL);
        }
        EBR__END();
    } else {
        for (int i = 0; i < num_threads; i++) {
            process(argv[1]);
        }
    }

    // Collapse results
    my_resize_barrier(&product_table);
    if (0) {
        nbhs_for(e, &product_table) {
            ProductEntry* p = *e;
            char* name = (char*) &p->key;
            double avg = (double) p->val.total / p->val.entries;
            printf(
                "%3s %d buy=%d sell=%d avg qty=%6.2f\n",
                name, p->val.entries, p->val.buys, p->val.sells, avg
            );
        }
    }

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
