# Track A: Ingest

The task, the input files and the exact output format are in the [spec](https://github.com/hackathon-raptors/speed-demon-2026/blob/main/specs/a-ingest.md).

## Approach

Read-only `mmap` lets workers access the prewarmed input without copying it into
an application buffer. Twelve workers split the file at newline boundaries and
index every consecutive run of records belonging to the same UTC hour. The index
records actual transitions; it does not assume timestamps arrive in order.

Workers then claim individual hours. Each hour uses a small endpoint hash table,
an exact-sized array of parsed records, and a contiguous uint32 latency array.
Counting and scattering records groups the latencies without per-group allocations.
A word-based hash avoids hashing every endpoint byte; full byte comparisons resolve
collisions. Nearest-rank selection finds p50, p95, and p99 without sorting entire
latency groups. Endpoints are sorted in ASCII byte order.

Workers format integers directly into hour buffers. A serialized writer emits the
completed prefix of hours in chronological order and frees those buffers immediately.
This overlaps output with computation and limits retained output memory. The only
file written is `summary.txt`.

The fixed 72-hour calendar span, endpoint limit, and uint32 latency bounds come from
the specification. Endpoint names, traffic distributions, and results are read from
the input on every run.

## Build and test locally

From the repository root, with a C11 compiler and POSIX system APIs:

```sh
make -C track-a
track-a/solution --data data/a/full-seed1 --out .out/track-a/full-seed1
python3 -m unittest discover -s tests -v
TRACK_A_SANITIZE=1 python3 -m unittest discover -s tests -v
```

The tests compile a temporary binary. They cover unordered hours, month rollover,
duplicates, uint32 extremes, percentile ranks, sampled-hash collisions, all methods,
and output errors. If the official seed-1 tiny, sample, and full datasets are under
`data/a/<scale>-seed1`, the suite also verifies their published output hashes.

For repeated native measurements with output verification:

```sh
python3 tools/bench_track_a.py track-a/solution \
  --data data/a/full-seed1 \
  --expected 495ca09682b9673ef836a506496177680bb4a3af54255174befddb1f83481cd3 \
  --json .out/track-a/benchmark.json
```

The benchmark warms the input, performs one warm-up execution, then measures seven
runs and discards the fastest and slowest. Supply multiple binaries to compare them
in alternating order. Local timings are for iteration; official K uses the scoring
machine. Local RSS includes the mapped input, whereas the harness excludes the
host-prewarmed input from its cgroup memory accounting.

Worker-count experiments use `make -B -C track-a WORKERS=6` (or `WORKERS=8`). The
default is 12. The scoring allocation has six physical P-cores with SMT, so fewer
workers may trade less contention and memory against available parallelism. Local
scaling on a Mac with twelve physical P-cores cannot settle that choice. Validate a
configuration with `TRACK_A_WORKERS=6 python3 -m unittest discover -s tests -v`.
For a board comparison, change the `WORKERS ?= 12` default in the Makefile before
the next submission; a local command-line override does not change the board build.

## Dependencies

Zero external runtime dependencies (`dependency_count = 0`, so D = 1). C11 and POSIX
memory mapping/thread APIs use the system C library. Python is only local test and
benchmark tooling. The submission builds offline with the pinned, allowlisted GCC
image; `-march=native` selects the scoring CPU when the board builds it.

## With another 24 hours

Profile on the scoring CPU, compare worker counts on its six physical P-cores, and
test a single parsing pass against the memory cost of retaining compact records.
Track changes in K, including formatter-normalized source size, rather than accepting
every runtime improvement. Keep testing additional generator seeds before submission.
