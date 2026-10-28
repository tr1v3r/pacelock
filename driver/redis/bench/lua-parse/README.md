# Redis Lua scalar vs Q2 experiment

This is an isolated experiment, not a proposed production protocol migration.
The scalar prototype drops cross-client policy-conflict detection and legacy
state compatibility; it is not contract-equivalent to Pacelock's Q2 protocol.

Run from the Redis driver module against an **isolated, disposable Redis**:

```sh
go run ./bench/lua-parse -addr 127.0.0.1:PORT > results.json
go run ./bench/lua-parse -addr 127.0.0.1:PORT -iterations 200000 > results-200k.json
```

The address must be explicit. Do not run against production or concurrently:
this harness overwrites/deletes fixed `bench:{lua-parse}:*` keys and relies on
exclusive EVALSHA statistics. It needs SCRIPT LOAD, EVALSHA, INFO, TIME, GET,
MSET and DEL permissions. Start Redis with `--save '' --appendonly no` to match
this run; do not change persistence settings on an existing service.

## Method

- Uses go-redis and the checkout's actual `protocol.lua` and `gcra.lua`.
- Three warmup rounds, then 15 rounds with deterministic randomized case order.
- Measures Redis `cmdstat_evalsha.usec` deltas, not client/network latency.
- Micro cases repeat 50,000 operations per script across 64 valid active TATs;
  all iterations contribute to a checked integer checksum. Fixture setup,
  loop/indexing, function-call and checksum overhead are included; setup is
  amortized and the common loop baseline is reported without subtraction.
- `scalar_parse`: `tonumber`; `q2_parse`: Q2 prefix/body extraction, pattern
  matching, and all three numeric conversions.
- Reader cases additionally validate bounds and allocate a state table; the Q2
  reader also checks the stored interval and burst against the current policy.
  Scalar validation is intentionally not identical for malformed input.
- Full cases run the actual Allow script with 1, 4, or 32 existing active quota
  keys. The scalar variant replaces the reader/encoder and removes policy
  comparison, while retaining GET, GCRA, MSET and response construction.
  These results measure **more than parsing**, not a parse-only speedup.
- Each full sample pipelines 1,000 calls, verifies every allowed response and
  checks the exact EVALSHA call-count delta. Fixture refresh is outside timing.
- Full cases use interval=1us and burst=1,000,000,000; micro cases use
  interval=100,000us and burst=10. They are not identical workloads.
- Reports the median of per-round averages, not request-latency percentiles.
  Lua GC, server scheduling, allocator behavior and VM noise remain included.
- A second run with 200,000 iterations checks microbenchmark scaling.

## Timing trap

An initial experiment called TIME twice in one Lua script and received the
same cached time, producing zero micro times. That invalid run is excluded.
The retained runs are [50,000 iterations](results-redis8-valid.json) and
[200,000 iterations](results-redis8-200k.json); both use commandstats.

No production implementation was modified.

## Local results (2026-10-28)

Tested production source: `2820790` (2026-10-28 checkout).
Apple M3 Pro / arm64, OrbStack Linux container, Redis 8.10.2, RDB/AOF disabled.
Medians of round averages from the corrected 50,000-iteration run:

| Micro case | Scalar | Q2 | Q2 overhead |
|---|---:|---:|---:|
| Parse | 851.80 ns | 1323.50 ns | 471.70 ns (+55.4%) |
| Read and validate | 1031.16 ns | 1540.14 ns | 508.98 ns (+49.4%) |

Common loop baseline: 35.18 ns. At 200,000 iterations: parse 887.40 / 1321.85 ns;
read/validate 1053.91 / 1544.74 ns. Extra cost remains about 0.43–0.51us/quota.

| Full Allow server execution | Scalar | Q2 | Q2 overhead |
|---|---:|---:|---:|
| 1 quota | 5.319 us | 6.234 us | 0.915 us (+17.2%) |
| 4 quotas | 13.676 us | 16.846 us | 3.170 us (+23.2%) |
| 32 quotas | 83.663 us | 109.052 us | 25.389 us (+30.3%) |

The relative parse difference is substantial, but the absolute increment is
about half a microsecond per quota. It can matter on CPU-bound multi-quota
paths. These percentages are not end-to-end latency or production-throughput
changes, and the scalar prototype is not a contract-preserving replacement.
