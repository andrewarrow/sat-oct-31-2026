#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

enum { HOURS = 72, MAX_ENDPOINTS = 1842, HASH_SLOTS = 4096, BUFFER_SIZE = 1 << 20 };

typedef struct {
    uint32_t *values;
    uint32_t count, capacity, errors;
} Group;

typedef struct {
    char name[126];
    size_t length;
    Group groups[HOURS];
} Endpoint;

static Endpoint endpoints[MAX_ENDPOINTS];
static Endpoint *table[HASH_SLOTS], *ordered[MAX_ENDPOINTS];
static size_t endpoint_count;
static char hours[HOURS][21], buffer[BUFFER_SIZE];

static void fail(const char *message) {
    fprintf(stderr, "ingest: %s\n", message);
    exit(EXIT_FAILURE);
}

static FILE *open_file(const char *directory, const char *name, const char *mode) {
    size_t size = strlen(directory) + strlen(name) + 2;
    char *path = malloc(size);
    if (!path)
        fail("out of memory");
    snprintf(path, size, "%s/%s", directory, name);
    FILE *file = fopen(path, mode);
    if (!file) {
        fprintf(stderr, "ingest: %s: %s\n", path, strerror(errno));
        exit(EXIT_FAILURE);
    }
    free(path);
    return file;
}

static Endpoint *find_endpoint(const char *name, size_t length, uint32_t hash) {
    size_t slot = hash & (HASH_SLOTS - 1);
    while (table[slot]) {
        Endpoint *endpoint = table[slot];
        if (endpoint->length == length && !memcmp(endpoint->name, name, length))
            return endpoint;
        slot = (slot + 1) & (HASH_SLOTS - 1);
    }
    if (endpoint_count == MAX_ENDPOINTS)
        fail("too many endpoints (specification limit is 1842)");
    Endpoint *endpoint = &endpoints[endpoint_count];
    ordered[endpoint_count++] = table[slot] = endpoint;
    memcpy(endpoint->name, name, length);
    endpoint->length = length;
    return endpoint;
}

/* The grammar guarantees complete, well-formed LF-terminated records here. */
static const char *parse_line(const char *line) {
    unsigned day = (unsigned)(line[8] - '0') * 10 + (unsigned)(line[9] - '0');
    /* The specified 72-hour span crosses October 31 into November 1. */
    unsigned days = line[6] == '0' ? day - 30 : day + 1;
    unsigned hour = days * 24 + (unsigned)(line[11] - '0') * 10 +
                    (unsigned)(line[12] - '0') - 18;
    if (!hours[hour][0]) {
        memcpy(hours[hour], line, 13);
        memcpy(hours[hour] + 13, ":00:00Z", 8);
    }
    const char *p = line + 28; /* Skip the fixed-width timestamp and space. */
    while (*p++ != ' ')       /* Ignore the method. */
        ;
    const char *name = p;
    uint32_t hash = UINT32_C(2166136261);
    while (*p != ' ') {
        hash = (hash ^ (unsigned char)*p) * UINT32_C(16777619);
        ++p;
    }
    Endpoint *endpoint = find_endpoint(name, (size_t)(p - name), hash);
    Group *group = &endpoint->groups[hour];
    group->errors += p[1] == '5';
    p += 5; /* Space, three status digits, space. */
    uint32_t latency = 0;
    while (*p != '\n')
        latency = latency * 10 + (unsigned)(*p++ - '0');
    if (group->count == group->capacity) {
        uint32_t capacity = group->capacity ? group->capacity * 2 : 4;
        uint32_t *values = realloc(group->values, (size_t)capacity * sizeof(*values));
        if (!values)
            fail("out of memory");
        group->values = values;
        group->capacity = capacity;
    }
    group->values[group->count++] = latency;
    return p + 1;
}

static void ingest(FILE *input) {
    size_t carry = 0;
    while (!feof(input)) {
        size_t bytes = fread(buffer + carry, 1, sizeof(buffer) - carry, input);
        if (ferror(input))
            fail("could not read requests.log");
        const char *end = buffer + carry + bytes, *stop = end;
        while (stop > buffer && stop[-1] != '\n')
            --stop;
        const char *p = buffer;
        while (p < stop)
            p = parse_line(p);
        carry = (size_t)(end - stop);
        if (carry == sizeof(buffer))
            fail("input record exceeds the buffer size");
        memmove(buffer, stop, carry);
    }
    if (carry)
        fail("requests.log must end with a newline");
}

static void swap(uint32_t *a, uint32_t *b) {
    uint32_t value = *a;
    *a = *b;
    *b = value;
}

/* Three-way quickselect keeps equal values together, including all-equal groups. */
static uint32_t select_rank(uint32_t *values, size_t count, size_t rank) {
    size_t lo = 0, hi = count;
    while (hi - lo > 1) {
        uint32_t a = values[lo], b = values[lo + (hi - lo) / 2], c = values[hi - 1];
        if (a > b)
            swap(&a, &b);
        if (b > c)
            swap(&b, &c);
        if (a > b)
            swap(&a, &b);
        uint32_t pivot = b;
        size_t less = lo, scan = lo, greater = hi;
        while (scan < greater) {
            if (values[scan] < pivot)
                swap(&values[less++], &values[scan++]);
            else if (values[scan] > pivot)
                swap(&values[scan], &values[--greater]);
            else
                ++scan;
        }
        if (rank < less)
            hi = less;
        else if (rank >= greater)
            lo = greater;
        else
            return pivot;
    }
    return values[lo];
}

static int compare_endpoints(const void *a, const void *b) {
    const Endpoint *left = *(Endpoint *const *)a;
    const Endpoint *right = *(Endpoint *const *)b;
    return strcmp(left->name, right->name);
}

static void write_summary(FILE *output) {
    const unsigned percentiles[] = {50, 95, 99};
    qsort(ordered, endpoint_count, sizeof(*ordered), compare_endpoints);
    for (unsigned hour = 0; hour < HOURS; ++hour) {
        for (size_t e = 0; e < endpoint_count; ++e) {
            Group *group = &ordered[e]->groups[hour];
            if (!group->count)
                continue;
            uint32_t result[3];
            size_t base = 0;
            for (size_t i = 0; i < 3; ++i) {
                size_t rank = ((uint64_t)percentiles[i] * group->count + 99) / 100 - 1;
                result[i] = select_rank(group->values + base, group->count - base, rank - base);
                base = rank; /* Higher percentiles only need the remaining upper partition. */
            }
            uint64_t ppm = (uint64_t)group->errors * 1000000 / group->count;
            if (fprintf(output, "%s %s %" PRIu32 " %" PRIu32 " %" PRIu32 " %" PRIu32
                                " %" PRIu32 " %" PRIu64 "\n",
                        hours[hour], ordered[e]->name, group->count, result[0], result[1],
                        result[2], group->errors, ppm) < 0)
                fail("could not write summary.txt");
        }
    }
}

int main(int argc, char **argv) {
    const char *data_dir = "/data", *out_dir = "/out";
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--help")) {
            puts("Usage: ingest [--data DIRECTORY] [--out DIRECTORY]");
            return EXIT_SUCCESS;
        }
        if (i + 1 < argc && !strcmp(argv[i], "--data")) {
            data_dir = argv[++i];
        } else if (i + 1 < argc && !strcmp(argv[i], "--out")) {
            out_dir = argv[++i];
        } else {
            fail("expected --data DIRECTORY or --out DIRECTORY (see --help)");
        }
    }
    FILE *input = open_file(data_dir, "requests.log", "rb");
    ingest(input);
    if (fclose(input))
        fail("could not close requests.log");
    if (mkdir(out_dir, 0777) && errno != EEXIST)
        fail("could not create the output directory");
    FILE *output = open_file(out_dir, "summary.txt", "wb");
    if (setvbuf(output, NULL, _IOFBF, BUFFER_SIZE))
        fail("could not buffer summary.txt");
    write_summary(output);
    if (fclose(output))
        fail("could not finish writing summary.txt");
    for (size_t e = 0; e < endpoint_count; ++e)
        for (unsigned hour = 0; hour < HOURS; ++hour)
            free(endpoints[e].groups[hour].values);
    return EXIT_SUCCESS;
}
