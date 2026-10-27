# Local fixture observations

Historical native-TTL/Pending protocol measurements, not benchmarks of the new
MSET/logical-expiry protocol. Current physical memory drain requires scheduled cleanup.
See [the MSET verification record](redis-mset-local.md) for the new protocol.

These are measurements of the development implementation based on
`27e91ca7dd916b8bc63c35b68266a9ba535236a8` (Go 1.24.13, macOS/arm64, Apple M3 Pro,
host-to-Docker loopback, Redis 7.4.11). The repository Compose fixture has no
persistence; Cluster has three masters without replicas. The separate failover
harness provisions a primary and replica without persistence and manually promotes.
Raw load results include dependency/build metadata; the tool source was uncommitted
at measurement time. Reproduce with the source shipped alongside this record.

- `load-local.jsonl`: 36 one-second matrix smoke samples (100 ms warmup, eight
  workers), then a 30-second hot-domain sample (one-second warmup, 16 workers).
  These short local samples validate instrumentation, not a production capacity
  envelope. No other benchmark should run concurrently for target acceptance.
- `cluster-local.json`: five-second, eight-domain, 32-rule Cluster smoke run with
  one-second warmup, two-second retention and post-retention observation.
- `fault-local.json`: replicated and partitioned-replica manual promotions, real
  noeviction OOM and synthetic Lua-clock tests. All ten replicated records survived
  the controlled promotion; all ten unreplicated records were missing after stale
  promotion, allowing ten additional admissions. OOM produced ten Unknown errors.

Do not turn these figures into a throughput promise. Phase end stops scheduling and drains pending calls; any request errors are
included in totals. The records preserve error counts, latency
sampling method, record counts, memory and CPU observations. See
[deployment validation](../deployment-validation.md) for reproduction and the
remaining environment-specific gates.

## Captured observations

Recorded on 2026-10-24 (Asia/Singapore). The final sequential standalone run
reported zero errors in all 37 samples. The 30-second, 32-rule, one-domain
AllowOnce sample measured approximately 5568 operations/second, with sampled
P50/P95/P99 of 2757/3956/5501 microseconds. This is a local observation only.
Its namespace had 28830 observed keys after load and 0 after the retention wait.

- once, four rules, one domain: observed keys 22009 → 0 after retention; used Redis memory 10713904 → 1595304 bytes.
- once-deny, four rules, one domain: observed keys 24966 → 4 after retention; used Redis memory 11837656 → 1585744 bytes.

The eight-domain Cluster smoke sample completed 41063 operations with zero errors;
observed keys fell from 16470 to 0 after retention. Per-master INFO snapshots are
in the raw JSON.
