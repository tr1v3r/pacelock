# Redis Lua scalar vs tuple benchmark

This directory preserves historical static tuple/scalar experiments, not current dynamic-admission performance. The runnable harness has been removed because its caller-policy protocol no longer exists. To reproduce the tuple experiment, use the harness and Lua together from commit `bcc67d5` in a separate checkout against a disposable Redis. Never run it against current policy/debt keys.

## Method

- Three excluded warmup rounds, then 15 rounds in deterministic randomized order.
- Redis `cmdstat_evalsha.usec` deltas exclude client/network latency. Two TIME calls
  within one script may return the same cached time and are not a valid timer.
- Micro cases repeat 50,000 operations over 64 active TATs. Every result contributes
  to a checked checksum. Setup, loop/indexing, function calls and checksum work are
  included; setup is amortized and the loop baseline is reported, not subtracted.
- `scalar_parse` uses `tonumber`; `tuple_parse` matches the three decimal fields and
  converts each. Reader cases also check bounds and allocate state tables; the
  tuple reader checks stored interval/burst. Malformed-input validation differs.
- Full Allow cases use the actual script with 1, 4 or 32 active quotas. The scalar
  variant replaces state decoding/encoding and removes policy comparison; GET,
  GCRA, MSET and response construction remain. This measures **more than parsing**.
- Each full sample pipelines 1,000 verified allowed calls and checks the exact
  EVALSHA count delta. State refresh is outside timing. Full cases use interval=1us,
  burst=1,000,000,000; micro cases use interval=100,000us, burst=10.
- Reports medians of round averages, **not request P50/P99**. GC, allocation,
  scheduling and VM noise remain. Only hot, valid, active, allowed paths are covered.
- The optional 200,000-iteration run checks scaling. These are local experiments,
  not production-throughput promises or evidence of million-QPS capacity.

## Historical tuple measurements (2026-10-28)

[Raw results](results-redis8-tuple.json) record the Lua SHA-256 and then-current quota
format. Apple M3 Pro / arm64, OrbStack Linux, Redis 8.10.2, RDB/AOF disabled;
15 measured rounds, 50,000 micro iterations, 1,000 calls per full sample.

| Case | Scalar | Historical tuple |
|---|---:|---:|
| Parse | 935.14 ns | 1285.60 ns |
| Read and validate | 1128.92 ns | 1512.82 ns |
| Allow, 1 quota | 5.788 us | 6.804 us |
| Allow, 4 quotas | 14.455 us | 17.651 us |
| Allow, 32 quotas | 89.831 us | 112.976 us |

Loop baseline: 37.54 ns. These are not controlled before/after measurements
against the historical run below; cross-run differences do not isolate tag removal.

## Historical Q2 measurements (2026-10-28)

The raw [50k](results-redis8-valid.json) and [200k](results-redis8-200k.json) datasets
are retained evidence from production source `2820790` and harness commit `c331f56`.
They measure the **removed Q2 format**, not the later tuple. Historical code is
available in Git; the current driver retains only legacy tuple debt reading, not the old admission path.

Apple M3 Pro / arm64, OrbStack Linux, Redis 8.10.2, RDB/AOF disabled. Medians of
round averages from the corrected 50k run:

| Historical micro case | Scalar | Q2 | Q2 overhead |
|---|---:|---:|---:|
| Parse | 851.80 ns | 1323.50 ns | 471.70 ns (+55.4%) |
| Read and validate | 1031.16 ns | 1540.14 ns | 508.98 ns (+49.4%) |

Loop baseline: 35.18 ns. The 200k run measured parse 887.40 / 1321.85 ns and
read/validate 1053.91 / 1544.74 ns.

| Historical Allow server execution | Scalar | Q2 |
|---|---:|---:|
| 1 quota | 5.319 us | 6.234 us |
| 4 quotas | 13.676 us | 16.846 us |
| 32 quotas | 83.663 us | 109.052 us |

Do not attribute these historical differences solely to the removed prefix or
claim them as current performance. The scalar comparison also removes policy
checks and changes encoding/storage.
