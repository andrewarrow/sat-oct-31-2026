#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef WORKERS
#define WORKERS 12
#endif
_Static_assert(WORKERS >= 1 && WORKERS <= 12, "WORKERS must be between 1 and 12");
enum { HOURS = 72, MAX_ENDPOINTS = 1842, HASH_SLOTS = 4096 };

typedef struct { const char *begin, *end; } Run;
typedef struct {
    Run *runs;
    size_t count, capacity, records, length;
    char stamp[21], *output;
} Hour;
typedef struct {
    const char *name;
    uint32_t length, count, errors, begin, next;
} Group;
typedef struct { uint32_t slot, latency; } Record;

static const char *input;
static size_t input_size;
static Hour hours[HOURS];
static pthread_mutex_t index_lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_uint next_hour;
static FILE *output;
static unsigned next_write;

static void fail(const char *message) {
    fprintf(stderr, "ingest: %s\n", message);
    exit(EXIT_FAILURE);
}

static void *resize(void *memory, size_t bytes) {
    void *result = realloc(memory, bytes);
    if (!result)
        fail("out of memory");
    return result;
}

static FILE *open_file(const char *directory, const char *name, const char *mode) {
    size_t size = strlen(directory) + strlen(name) + 2;
    char *path = resize(NULL, size);
    snprintf(path, size, "%s/%s", directory, name);
    FILE *file = fopen(path, mode);
    if (!file) {
        fprintf(stderr, "ingest: %s: %s\n", path, strerror(errno));
        exit(EXIT_FAILURE);
    }
    free(path);
    return file;
}

static unsigned hour_id(const char *line) {
    unsigned day = (unsigned)(line[8] - '0') * 10 + (unsigned)(line[9] - '0');
    unsigned days = line[6] == '0' ? day - 30 : day + 1;
    return days * 24 + (unsigned)(line[11] - '0') * 10 + (unsigned)(line[12] - '0') - 18;
}

static const char *boundary(size_t offset) {
    if (!offset || offset == input_size)
        return input + offset;
    return (const char *)memchr(input + offset - 1, '\n', input_size - offset + 1) + 1;
}

/* Index actual equal-hour runs; no assumption about timestamp order or shard lag. */
static void *index_input(void *argument) {
    size_t worker = (uintptr_t)argument;
    const char *p = boundary(input_size * worker / WORKERS);
    const char *end = boundary(input_size * (worker + 1) / WORKERS);
    while (p < end) {
        const char *begin = p;
        unsigned id = hour_id(p);
        size_t records = 0;
        do {
            ++records;
            p = (const char *)memchr(p + 32, '\n', (size_t)(end - p - 32)) + 1;
        } while (p < end && hour_id(p) == id);
        pthread_mutex_lock(&index_lock);
        Hour *hour = &hours[id];
        if (hour->count == hour->capacity) {
            hour->capacity = hour->capacity ? hour->capacity * 2 : 16;
            hour->runs = resize(hour->runs, hour->capacity * sizeof(*hour->runs));
        }
        hour->runs[hour->count++] = (Run){begin, p};
        hour->records += records;
        memcpy(hour->stamp, begin, 13);
        memcpy(hour->stamp + 13, ":00:00Z", 8);
        pthread_mutex_unlock(&index_lock);
    }
    return NULL;
}

/* Mix a few words, then check the full path on every match. Collisions are harmless. */
static uint64_t endpoint_hash(const char *name, size_t length) {
    uint64_t a = 0, b = 0, c = 0;
    if (length >= 8) {
        memcpy(&a, name, 8);
        memcpy(&b, name + (length - 8) / 2, 8);
        memcpy(&c, name + length - 8, 8);
    } else {
        memcpy(&a, name, length);
    }
    uint64_t hash = (a ^ b * UINT64_C(0x9e3779b185ebca87) ^ c) + length;
    hash ^= hash >> 29;
    hash *= UINT64_C(0xbf58476d1ce4e5b9);
    return hash ^ (hash >> 32);
}

static int compare_groups(const void *a, const void *b) {
    const Group *left = *(Group *const *)a, *right = *(Group *const *)b;
    size_t length = left->length < right->length ? left->length : right->length;
    int order = memcmp(left->name, right->name, length);
    return order ? order : (left->length > right->length) - (left->length < right->length);
}

static uint32_t select_rank(uint32_t *values, int count, int rank) {
    int lo = 0, hi = count - 1;
    while (hi - lo > 12) {
        uint32_t pivot = values[lo + (hi - lo) / 2];
        int i = lo, j = hi;
        do {
            while (values[i] < pivot)
                ++i;
            while (values[j] > pivot)
                --j;
            if (i <= j) {
                uint32_t value = values[i];
                values[i++] = values[j];
                values[j--] = value;
            }
        } while (i <= j);
        if (rank <= j)
            hi = j;
        else if (rank >= i)
            lo = i;
        else
            return values[rank];
    }
    for (int i = lo + 1; i <= hi; ++i) {
        uint32_t value = values[i];
        int j = i;
        while (j > lo && values[j - 1] > value) {
            values[j] = values[j - 1];
            --j;
        }
        values[j] = value;
    }
    return values[rank];
}

static char *put_uint(char *out, uint32_t value) {
    char digits[10];
    unsigned count = 0;
    do {
        digits[count++] = (char)('0' + value % 10);
        value /= 10;
    } while (value);
    while (count)
        *out++ = digits[--count];
    return out;
}

static void process_hour(Hour *hour) {
    Group groups[HASH_SLOTS] = {0}, *ordered[MAX_ENDPOINTS];
    unsigned group_count = 0;
    Record *records = resize(NULL, hour->records * sizeof(*records));
    uint32_t *values = resize(NULL, hour->records * sizeof(*values));
    size_t record = 0;
    for (size_t r = 0; r < hour->count; ++r) {
        const char *p = hour->runs[r].begin, *end = hour->runs[r].end;
        while (p < end) {
            p += 31; /* Timestamp, space, and the minimum three method bytes. */
            while (*p++ != ' ')
                ;
            const char *name = p;
            p = memchr(name, ' ', (size_t)(end - name));
            size_t length = (size_t)(p - name);
            size_t slot = endpoint_hash(name, length) & (HASH_SLOTS - 1);
            while (groups[slot].name && (groups[slot].length != length ||
                   memcmp(groups[slot].name, name, length)))
                slot = (slot + 1) & (HASH_SLOTS - 1);
            Group *group = &groups[slot];
            if (!group->name) {
                if (group_count == MAX_ENDPOINTS)
                    fail("too many endpoints (specification limit is 1842)");
                group->name = name;
                group->length = (uint32_t)length;
                ordered[group_count++] = group;
            }
            ++group->count;
            group->errors += p[1] == '5';
            p += 5;
            uint32_t latency = 0;
            while (*p != '\n')
                latency = latency * 10 + (unsigned)(*p++ - '0');
            ++p;
            records[record++] = (Record){(uint32_t)slot, latency};
        }
    }
    uint32_t offset = 0;
    for (unsigned e = 0; e < group_count; ++e) {
        Group *group = ordered[e];
        group->begin = group->next = offset;
        offset += group->count;
    }
    for (size_t i = 0; i < record; ++i)
        values[groups[records[i].slot].next++] = records[i].latency;
    free(records);
    qsort(ordered, group_count, sizeof(*ordered), compare_groups);
    char *begin = resize(NULL, (size_t)group_count * 224), *out = begin;
    const unsigned percentiles[] = {50, 95, 99};
    for (unsigned e = 0; e < group_count; ++e) {
        Group *group = ordered[e];
        uint32_t numbers[6] = {group->count, 0, 0, 0, group->errors,
                              (uint32_t)((uint64_t)group->errors * 1000000 / group->count)};
        int base = 0;
        for (unsigned i = 0; i < 3; ++i) {
            int rank = (int)(((uint64_t)percentiles[i] * group->count + 99) / 100 - 1);
            numbers[i + 1] = i && rank == base ? numbers[i] :
                select_rank(values + group->begin + base, (int)group->count - base, rank - base);
            base = rank;
        }
        memcpy(out, hour->stamp, 20);
        out += 20;
        *out++ = ' ';
        memcpy(out, group->name, group->length);
        out += group->length;
        for (unsigned i = 0; i < 6; ++i) {
            *out++ = ' ';
            out = put_uint(out, numbers[i]);
        }
        *out++ = '\n';
    }
    free(values);
    /* Publish complete hours and drain the ready prefix in chronological order. */
    pthread_mutex_lock(&index_lock);
    hour->output = begin;
    hour->length = (size_t)(out - begin);
    while (next_write < HOURS && (!hours[next_write].records || hours[next_write].output)) {
        Hour *ready = &hours[next_write++];
        if (ready->length && fwrite(ready->output, 1, ready->length, output) != ready->length)
            fail("could not write summary.txt");
        free(ready->output);
        ready->output = NULL;
    }
    pthread_mutex_unlock(&index_lock);
}

static void *summarize(void *unused) {
    (void)unused;
    unsigned id;
    while ((id = atomic_fetch_add_explicit(&next_hour, 1, memory_order_relaxed)) < HOURS)
        if (hours[id].records)
            process_hour(&hours[id]);
    return NULL;
}

static void parallel(void *(*function)(void *)) {
    pthread_t workers[WORKERS];
    for (uintptr_t i = 0; i < WORKERS - 1; ++i)
        if (pthread_create(&workers[i], NULL, function, (void *)i))
            fail("could not start worker thread");
    function((void *)(uintptr_t)(WORKERS - 1));
    for (unsigned i = 0; i < WORKERS - 1; ++i)
        if (pthread_join(workers[i], NULL))
            fail("could not join worker thread");
}

int main(int argc, char **argv) {
    const char *data_dir = "/data", *out_dir = "/out";
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--help")) {
            puts("Usage: ingest [--data DIRECTORY] [--out DIRECTORY]");
            return EXIT_SUCCESS;
        }
        if (i + 1 < argc && !strcmp(argv[i], "--data"))
            data_dir = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--out"))
            out_dir = argv[++i];
        else
            fail("expected --data DIRECTORY or --out DIRECTORY (see --help)");
    }
    FILE *source = open_file(data_dir, "requests.log", "rb");
    struct stat info;
    if (fstat(fileno(source), &info) || !S_ISREG(info.st_mode))
        fail("could not read requests.log as a regular file");
    input_size = (size_t)info.st_size;
    if (input_size) {
        input = mmap(NULL, input_size, PROT_READ, MAP_PRIVATE, fileno(source), 0);
        if (input == MAP_FAILED)
            fail("could not map requests.log");
        if (input[input_size - 1] != '\n')
            fail("requests.log must end with a newline");
        parallel(index_input);
    }
    if (mkdir(out_dir, 0777) && errno != EEXIST)
        fail("could not create the output directory");
    output = open_file(out_dir, "summary.txt", "wb");
    if (input_size)
        parallel(summarize);
    for (unsigned h = 0; h < HOURS; ++h)
        free(hours[h].runs);
    if (fclose(output))
        fail("could not finish writing summary.txt");
    if (input_size && munmap((void *)input, input_size))
        fail("could not unmap requests.log");
    if (fclose(source))
        fail("could not close requests.log");
    return EXIT_SUCCESS;
}
