# Track A: performance log

Official runs below come from the [live board feed](https://s3.amazonaws.com/raptors.dev/speeddemon.dev/2026/board/board.json).
Local experiments are listed separately; they are not official scores.

| Step | Change | Run ID | T (s) | M (MB) | S | D | K | Points |
|---|---|---|---|---|---|---|---|---|---|
| 0 | Published Python reference | Published reference | 32.05 | 228 | 56 | 1 | 1 | 0 |
| 1 | Buffered C parser, FNV endpoint hash, per-group vectors, three-way selection, stdio output | `run-07717a8d52f7` | 0.404863 | 34.685 | 228 | 1 | 10.503 | 51.067 |
| 2 | Parallel mmap hour-run index, allocation-free per-group storage, sampled-word hash, two-way selection, direct integer formatting, streaming output | `run-0b8199fad5f2` | 0.048016 | 24.556 | 334 | 1 | 30.862 | 74.471 |

Step 1 is commit `9d95121e5cf959f3fe321d80c2c473a11cd6cf90`.
Step 2 is commit `0f8f65587932f71e820a1bde3d43ad610771951c`.
Both passed every correctness and sandbox gate. Step 2 improved runtime 8.43x and
K 2.94x, moving from 75th to 34th in the October 31, 13:08 UTC snapshot.

## Why these changes

The original C implementation had already eliminated external dependencies. Further
dependency savings were unavailable. Its bottlenecks were serial endpoint hashing,
many small reallocations, and serial selection/formatting.

The rewrite uses the allowed threads for both indexing and hour tasks. It retains
only the latency arrays for active hours and releases completed output promptly.
The endpoint hash samples three words, but full comparisons still establish equality.
No endpoint catalogue or output is precomputed.

The source increase from 228 to 334 lines costs `(228 / 334)^0.15 = 0.9443` in K,
about 5.6%. The measured time and memory gains more than cover it. Counts were checked
locally using the harness versions, clang-format 23.1.3 with LLVM style and scc 4.1.0;
the baseline count reproduces the board's 228 exactly.

## Local experiments

Apple M4 Max, macOS arm64, Apple Clang 21, `-O3 -march=native`, official seed-1 full
input. Every candidate below matched the published full output hash. Repeated runs
used warmed inputs and trimmed means; results include native process startup.

| Experiment | Local time | Decision |
|---|---|---|
| Original C with the same compiler flags | 177.8 ms | Baseline |
| Parallel hours, six workers | 37.5 ms | Keep for worker-count comparison on the scoring CPU |
| Parallel hours, eight workers | 30.4 ms | Keep for worker-count comparison on the scoring CPU |
| Parallel hours, twelve workers | 24.0 ms | Initial rewrite |
| Stream completed hours while other workers compute | 22–23 ms | Keep; also lowers memory retention |
| Custom integer formatting on the original serial implementation | 9.4% faster | Keep in the parallel rewrite |
| Explicit SIMD newline search | About 2.4% faster | Reject: source growth and architecture branches for a small gain |
| Bulk decimal parsing | About 2.1% faster | Reject: marginal gain after source cost |
| One worker team with a barrier between phases | No repeatable improvement | Reject |

Local `mmap` RSS includes the input and must not be substituted for scored M.
The official harness uses cgroup memory accounting and prewarms the input outside
the run's cgroup. Output page cache still counts.

## Next target

At the captured board snapshot, tenth place has K = 39.973. With step 2's M and S,
matching it needs about 28.6 ms on the scoring CPU. At 20 MB and S = 334 the target
is about 31.1 ms. The next substantial experiment is eliminating the separate
indexing pass while keeping intermediate records compact. Unknown holdout seeds and
arbitrary timestamp disorder remain correctness requirements.
