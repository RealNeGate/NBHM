// I realize that I could reuse this for testing future concurrent algorithms
// so I'm factoring it out :)
#ifndef MC_H
#define MC_H

#ifndef MC_TESTING
#define MC_SEQ(op)
#define MC_REPORT_BEGIN()
#define MC_REPORT_READ(k, v) (v)
#define MC_REPORT_WRITE(k, v)
#else
#define MC_SEQ(op) MC_seq(__FILE__, __LINE__, op)
#define MC_REPORT_BEGIN() MC_report_begin()
#define MC_REPORT_READ(k, v) MC_report_read(k, v)
#define MC_REPORT_WRITE(k, v) MC_report_write(k, v)

// call this before atomic/shared state operations, this is what
// allows us to context switch and generate the nasty scheduling.
void MC_seq(const char* file, int line, const char* op);

// track when the latest operation starts, this is used to know
// when read or write reports are overlapping.
void MC_report_begin(void);

// report linearization point of read K,V. This will trigger
// an error if the K,V pair is either not visible between the
// last MC_report_begin and now OR the K,V pair contradicts a
// strictly ordered report.
//
// returns the v, just for convenience elsewhere
void* MC_report_read(void* k, void* v);

// report linearization point of write K,V.
void MC_report_write(void* k, void* v);
#endif // MC_TESTING
#endif // MC_H

#if defined(MC_IMPL) && defined(MC_TESTING)
typedef struct Report Report;
struct Report {
    Report* prev;

    int start, end;
    bool is_read;
    void *k, *v;
};

static atomic_int current_tick = 0;
static atomic_int active_ticket = 0;
static thread_local int my_ticket = 0;

static thread_local int op_start = 0;

static thread_local int sleeb = 0;
static atomic_int stagger = 3;

static thread_local uint64_t mc_seed;

extern int num_threads;

_Atomic(Report*) last_report = NULL;

void MC_seq(const char* file, int line, const char* op) {
    sleeb = (pcg32_pie(&mc_seed) % 15) + 1;

    for (;;) {
        // wait for ticket
        if (active_ticket == my_ticket) {
            if (sleeb == 0 && --stagger > 0) {
                break;
            }
            sleeb--;
            stagger = (pcg32_pie(&mc_seed) % 3) + 1;
            active_ticket = (active_ticket + 1) % num_threads;
        }
        thrd_yield();
    }

    int t = ++current_tick;
    printf("SEQ%d: T=%5d: %s:%d %s\n", my_ticket, t, file, line, op);
    fflush(stdout);
}

void MC_report_begin(void) {
    op_start = current_tick + 1;

    // printf("=== REPORT %d T=%d ===\n", my_ticket, op_start);
    // fflush(stdout);
}

void MC_report_read(void* k, void* v) {
    assert(k && k != NBHM_TOMBSTONE);

    int start = op_start;
    int end = current_tick + 1;

    printf("RD%d:  T=%5d: (%p, %p)\n", my_ticket, end, k, v);
    fflush(stdout);

    bool match = false;
    Report* curr = last_report;
    while (curr) {
        if (curr->k == k) {
            if (curr->end < start) {
                // our answer must be the same as the last strict reader or writer.
                match = (curr->v == v);
                break;
            } else if (curr->v == v) {
                // non-strict overlap, may pick this answer or a previous one
                match = true;
                break;
            }
        }
        curr = curr->prev;
    }

    if (!match && !(v == NULL && curr == NULL)) {
        printf("=== REPORT READ FAILURE ===\n");
        printf("  [%p] GOT %p, EXPECTED...\n", k, v);

        curr = last_report;
        bool fence = false;
        while (curr) {
            if (curr->k == k) {
                if (!fence && curr->end < start) {
                    printf("  [%-8d, %8d) %p (STRICT %s)\n", curr->start, curr->end, curr->v, curr->is_read ? "READ" : "WRITE");
                    fence = true;
                } else if (curr->v == v) {
                    printf("  [%-8d, %8d) %p (%s)\n", curr->start, curr->end, curr->v, curr->is_read ? "READ" : "WRITE");
                }
            }
            curr = curr->prev;
        }

        printf("\n");
        fflush(stdout);
        __builtin_debugtrap();
        return;
    }

    Report* r = calloc(1, sizeof(Report));
    r->prev = last_report;
    r->is_read = true;
    r->start = start;
    r->end = end;
    r->k = k;
    r->v = v;
    last_report = r;
}

void MC_report_write(void* k, void* v) {
    assert(k && k != NBHM_TOMBSTONE);

    printf("WR%d:  T=%5d: (%p, %p)\n", my_ticket, current_tick, k, v);
    fflush(stdout);

    Report* r = calloc(1, sizeof(Report));
    r->prev = last_report;
    r->is_read = false;
    r->start = op_start;
    r->end = current_tick + 1;
    r->k = k;
    r->v = v;
    last_report = r;
}

#endif // MC_IMPL
