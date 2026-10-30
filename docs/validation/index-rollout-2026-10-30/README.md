# PostgreSQL recovery-index rollout validation

Validation-only study of the recovery-time index added to Pacelock. Runtime code
is unchanged. Source baseline: `97e2d5d`; measured on 2026-10-30 (Asia/Singapore).
Results are local closed-loop observations, not production SLOs or a capacity
forecast. The index accelerates recovered-candidate lookup but prevents HOT
updates when indexed debt timestamps change. This study measures that tradeoff.

## Environment and reproducibility

- Apple M3 Pro, Mac15,7, 12 logical CPUs, 36 GiB RAM; macOS 26.5.2 (25F84).
- Docker 29.8.2 / OrbStack, aarch64 overlay2; VM 12 CPUs and 25,232,343,040 bytes RAM.
- PostgreSQL 17.11, image `postgres:17-alpine`, digest
  `sha256:eee6c6e1b0894fa4fe0075eb054af9fd1cf39a35321c2d4110626cfe506411fb`.
- Go 1.27.3, GOMAXPROCS=12. macOS client connects over loopback to the container.
- Instance dedicated to this experiment. Airlock load testing was paused during
  sampling. Other OS activity was not eliminated.
- `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `wal_level=replica`,
  `shared_buffers=128MB`, `work_mem=4MB`, `maintenance_work_mem=64MB`,
  `max_connections=100`, `max_wal_size=1GB`, `checkpoint_timeout=30min`.
- Autovacuum disabled **only in this disposable experiment**, to make short-run
  update/HOT and bloat observations easier to compare. Do not copy this setting
  into production. A manual CHECKPOINT precedes each write warmup.

Create a separate disposable server (never point the command at a production or
shared database). The command creates and drops only `pacelock_validation_*`
schemas, but it also runs CHECKPOINT and reads instance WAL positions.

```sh
docker run -d --name pacelock-index-rollout-pg \
  -e POSTGRES_USER=pacelock -e POSTGRES_PASSWORD=pacelock-test-only \
  -e POSTGRES_DB=pacelock -p 127.0.0.1::5432 \
  postgres:17-alpine -c autovacuum=off -c checkpoint_timeout=30min
docker port pacelock-index-rollout-pg
# Set DSN to the printed loopback port; these credentials are disposable fixtures.
export DSN='postgres://pacelock:pacelock-test-only@127.0.0.1:PORT/pacelock?sslmode=disable'
cd tests/contracts
go build -o /tmp/pacelock-indexrollout ./cmd/indexrollout
/tmp/pacelock-indexrollout -dsn "$DSN" -mode write -duration 10s -warmup 1s \
  -background-rows 100000 -rows 100000 -trials 3 -workers 8 -fillfactor 100
/tmp/pacelock-indexrollout -dsn "$DSN" -mode write -duration 10s -warmup 1s \
  -background-rows 100000 -rows 100000 -trials 3 -workers 8 -fillfactor 80
/tmp/pacelock-indexrollout -dsn "$DSN" -mode prune -rows 100000 -trials 3 -fillfactor 100
/tmp/pacelock-indexrollout -dsn "$DSN" -mode ddl -rows 100000 -trials 3 -fillfactor 100
# Remove only this dedicated fixture when done.
docker rm -f pacelock-index-rollout-pg
```

Run `python3 docs/validation/index-rollout-2026-10-30/summarize.py` from the
repository root to check accounting/row-count invariants and print per-trial
summaries from the committed raw JSONL.

## Method and measurement boundaries

**Write workload.** Three repetitions, index absent/present in AB/BA/AB order.
Every case gets a freshly created schema, identical rows and fillfactor. Each
of eight workers owns a separate DB handle with exactly one connection and
executes public `Limiter.Allow`; requests use a fixed-seed quota/scope selection
stream. Hot case: 1 active scope, 1 quota and 1 charge per transaction. Spread
case: 16 scopes, 32 quota names per scope, 4 distinct charges per transaction.
Both formal cases also contain 100,000 unmodified active debts in a separate
background scope (100,001 / 100,512 total rows), so the recovery index is not a
one-row toy. Policies allow all requests; any denial/error or update count other
than `allowed × dimensions` invalidates the run. Pure `EvaluateManaged` timing in
the pilot is a separate no-I/O diagnostic; an index cannot improve that function.

One-second warmup precedes each ten-second formal phase. Latencies measure each
Allow call including client/network/transaction overhead. Quantiles use
`floor((n-1)*q)` order statistics. Throughput divides completed calls by elapsed
closed-loop phase time, including last calls draining, stats flush, aggregation
and connection close. These fixed-concurrency clients have no external arrival
rate; latency excludes any upstream queue and must not be treated as an
open-loop overload latency distribution. Small-table, three-second pilot samples
are retained separately and are not pooled into formal results. Pilot DDL predates
the preconnected-probe and actual-row-count instrumentation; use `ddl.jsonl`
exclusively for DDL conclusions.

**Statistics.** Each worker calls `pg_stat_force_next_flush()` on the same single
connection used by its Driver, then closes it. Only after all workers finish does
the management connection clear its statistics snapshot and read counters. Raw
update deltas are checked against successful charges. WAL uses the difference of
`pg_current_wal_insert_lsn()` and is normalized per admission, not just per phase;
it is instance-wide and can include background WAL, not only tuple records.
`TableBytes` includes heap/TOAST/free-space/visibility structures, while PK and
recovery index sizes are separate. `ActualRows` is COUNT(*); `Live`/`Dead` are
PostgreSQL statistical estimates and must not be substituted for exact counts.
No long-run vacuum equilibrium or steady-state disk amplification is inferred.

**Prune workload.** 100,000 rows in one scope, either 0% or 1% recovered. Twenty-five
DELETE samples per case, each in a transaction that is rolled back to preserve
the same visible candidates. This warms cache and can leave aborted tuple/header
work; it is not a sequence of committed cleanup cycles. Index absent/present use
exactly the same new query; a third arm runs the old query without the index to
separate query shape from index effect. Timings cover SQL DELETE + its network
round trip, excluding BEGIN/COMMIT, driver domain-lock acquisition and backend
time lookup. They are **candidate/deletion path** latency, not full public Prune
call latency under contention.

**DDL workload.** Fresh 100,000-row table per method. Compare ordinary CREATE
INDEX, current `Driver.Migrate` (transactional), and CREATE INDEX CONCURRENTLY
(standalone, outside a transaction). In the controlled-blocking case a writer
holds a transaction; an observed DDL Lock wait is the barrier before a preconnected
second writer probes another row. After roughly 250ms the first writer commits.
Record probe lock-wait status, actual release time relative to probe start,
whether the probe finished before release, total DDL time, and final index
`indisvalid`/`indisready`. Total time includes the artificial blocker; separate
uncontended builds isolate build cost. This models queued DDL behavior, not a
production migration with arbitrary long snapshots or sustained arrivals.

## Results

All formal write phases had zero denials/errors, exact expected update counts,
and unchanged actual row counts. The strongest repeatable effect is **write
amplification and loss of HOT**, not a universal throughput regression.

### Admission writes (ten seconds per trial)

Values are medians of three trial metrics; brackets show the trial range. P99
is the median/range of per-trial p99 values, not a pooled request percentile.

| Fillfactor | Active workload | Recovery index | ops/s [range] | p50 ms | p95 ms | p99 ms [range] | WAL B/admit | HOT % |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 1 scope × 1 charge | no | 719 [669–728] | 10.60 | 14.47 | 24.12 [19.31–27.67] | 255 | 100.0 |
| 100 | 1 scope × 1 charge | yes | 681 [644–749] | 10.39 | 16.05 | 22.78 [17.64–43.27] | 686 | 0.0 |
| 100 | 16 scopes × 4 charges | no | 2185 [2055–2625] | 2.94 | 6.32 | 15.14 [6.88–22.35] | 767 | 99.9 |
| 100 | 16 scopes × 4 charges | yes | 2180 [1916–2398] | 3.11 | 6.33 | 10.04 [7.37–15.14] | 2814 | 0.0 |
| 80 | 1 scope × 1 charge | no | 711 [691–727] | 10.73 | 14.86 | 18.70 [16.97–23.31] | 313 | 100.0 |
| 80 | 1 scope × 1 charge | yes | 712 [661–786] | 11.14 | 14.25 | 24.49 [17.70–24.72] | 686 | 0.0 |
| 80 | 16 scopes × 4 charges | no | 2358 [2331–2458] | 2.95 | 5.64 | 8.39 [7.93–8.43] | 783 | 100.0 |
| 80 | 16 scopes × 4 charges | yes | 2294 [1868–2423] | 3.07 | 5.77 | 7.56 [7.27–17.27] | 2756 | 0.0 |

The separate single-charge pure `EvaluateManaged` pilot completed 100,000 calls
in 15.14 ms (about 151 ns/call). It contains no persistence and was not an
indexed/non-indexed comparison; no database-speed conclusion is drawn from it.

At fillfactor 100, WAL/admission rose about **2.69×** for one charge and
**3.66×** for four charges; HOT fell from 99.9–100% to 0% in every indexed
trial. The spread workload WAL/update was about 192 B without the index versus
703 B with it (admission WAL divided by four charged rows). Throughput and tail
latency overlap and sometimes reverse between paired trials; these measurements
do not establish a fixed slowdown percentage. Fillfactor 80 does not restore HOT
when the timestamp columns are indexed.

### Storage after the write phase

MiB, medians of three trials at fillfactor 100; autovacuum is disabled. These
are short-run sizes, not equilibrium bloat. Static background identifiers use a literal scope and 32-character MD5 quota IDs,
shorter than the SDK's 64-character hashes, so absolute bytes/row are fixture-specific.

| Workload | Index | Heap/aux MiB | PK MiB | Recovery index MiB | Estimated dead tuples |
| --- | --- | ---: | ---: | ---: | ---: |
| hot | no | 10.45 | 8.34 | 0.00 | 598 |
| hot | yes | 10.64 | 8.34 | 9.45 | 7036 |
| spread | no | 10.66 | 8.49 | 0.00 | 614 |
| spread | yes | 13.16 | 8.50 | 26.21 | 97200 |

The 100,000-row static Prune fixture occupied 9.68 MiB heap/aux and 7.45 MiB
primary key; the extra recovery index occupied about 7.36–7.38 MiB. Repeated
non-HOT write versions grew the recovery index to roughly 25–27 MiB in the
spread formal write case. Production vacuum/index cleanup may change this
substantially; disabling autovacuum here deliberately exposes short-run costs.

### Prune SQL path (100,000 rows)

Median of three per-trial quantiles. Each trial has 25 rolled-back DELETE calls.
At n=25, p99 is near the maximum observed sample; it is not a tail-latency SLO
or a statistical-significance estimate.

| Query | Index | Recovered | p50 ms | p95 ms | p99 ms |
| --- | --- | --- | ---: | ---: | ---: |
| current | no | 0% | 6.127 | 6.354 | 6.373 |
| current | no | 1% | 6.371 | 6.712 | 6.842 |
| current | yes | 0% | 0.270 | 0.355 | 0.379 |
| current | yes | 1% | 0.416 | 0.549 | 0.644 |
| legacy | no | 0% | 14.992 | 16.125 | 16.727 |
| legacy | no | 1% | 1.295 | 3.925 | 3.995 |

For the same current query, the index reduced p50 from about 6.13 to 0.27 ms
with no recovered rows and 6.37 to 0.42 ms with 1% recovered. Legacy-query results
depend strongly on recovered-row distribution and are a separate query-shape
comparison. No simultaneous admission-versus-Prune throughput test was performed.

### Index construction and queued writers

Three trials each. The first range is an uncontended build. The blocked scenario
holds a writer for about 253–254 ms after a preconnected probe starts; its DDL
total is **not** pure index construction time.

| Method | Uncontended build ms | Blocked DDL total ms | Probe write ms | Probe Lock observed | Probe finished before holder release |
| --- | ---: | ---: | ---: | --- | --- |
| create | 29.7–41.8 | 374.8–417.3 | 376.1–420.6 | all | none |
| migrate | 28.6–30.9 | 359.5–378.8 | 359.5–378.8 | all | none |
| concurrently | 37.2–38.2 | 365.3–398.0 | 7.1–7.2 | none | all |

Every completed index was valid and ready. Ordinary CREATE and current Migrate
queued later writes behind index creation; concurrent creation allowed the probe
to finish while the original writer was still held. The probe is a direct UPDATE,
not Allow; its roughly 7 ms concurrent result includes finding the row and its
own database work. This demonstrates the lock behavior, not an online-migration
latency guarantee.

**Recommendation:** retain the index for deployments with costly sparse/empty
cleanup scans, but budget the measured WAL/storage/non-HOT cost. For existing busy
tables, prefer a separately controlled concurrent build with validity checks;
do not treat Migrate as an online migration primitive. Validate with production
vacuum settings, more active quota cardinality, sustained mixed maintenance/admission
traffic and realistic network/replication before setting rollout thresholds.

## Deployment recommendation

1. Measure the hottest admission domain, cleanup lag, WAL/storage headroom and
   replica lag on the target deployment. Retain autovacuum and tune it for the
   additional non-HOT updates. Use the default fillfactor=100 results as the main
   observation; fillfactor=80 is a sensitivity test, not a recommended global change.
2. For a busy existing table, create the recovery index concurrently in a separate
   migration step, outside any transaction. Use the trusted application schema:

   ```sql
   CREATE INDEX CONCURRENTLY pacelock_quotas_recovered_idx
     ON application_schema.pacelock_quotas
       (scope_id, (GREATEST(expires_us, tat_us)), quota_id);
   ```

   Check `pg_index.indisvalid` and `indisready`, definition, table/schema, and disk
   headroom. A same-name invalid or incorrect index is not repaired by IF NOT
   EXISTS. Concurrent construction can wait on old writers/snapshots and still
   consumes I/O, CPU, WAL and storage; it is not zero-impact or timeout-free.
3. Current `Migrate` runs normal CREATE INDEX in a transaction. Use it for fresh
   databases or a controlled maintenance window. After provisioning the correct
   valid index separately, Migrate can observe that it exists; this does not make
   arbitrary future Migrate DDL an online operation. Test the exact deployment step.
4. Use bounded operational lock/statement timeouts and monitor
   `pg_stat_progress_create_index`, `pg_stat_activity` and `pg_locks`. A cancelled
   concurrent build may leave an invalid index. Inspect it and remove/rebuild it
   deliberately; do not assume the command rolled everything back.
5. Reducing or pausing Prune limits maintenance load but does not remove the
   recovery index maintenance, non-HOT updates or WAL incurred by each Allow.
   If that write cost is unacceptable, drop **only the recovery index** with DROP INDEX CONCURRENTLY
   outside a transaction after evaluating load. No quota/policy rows are deleted;
   accounting correctness remains. Prune will lose its access path, and the new
   query can scan/sort active debt again, so this is not a free performance rollback.
   Subsequent current Migrate calls recreate a missing recovery index, so coordinate
   migration/deployment steps to keep an intentional rollback from being undone.
   Never delete policies, reset TAT or lower revisions as an index rollback.

No production database was touched. No runtime change, push or PR was performed.

## Primary references

The observed HOT mechanism agrees with PostgreSQL 17's
[HOT documentation](https://www.postgresql.org/docs/17/storage-hot.html).
The deployment procedure follows the documented limitations of
[CREATE INDEX / concurrent builds](https://www.postgresql.org/docs/17/sql-createindex.html):
normal creation blocks writes, concurrent creation uses additional phases and can
leave an invalid index after failure, and IF NOT EXISTS does not verify equivalence.

## Checks and cleanup

- Go 1.27.3 `scripts/check.sh` passed across all six modules: gofmt, module
  verification, vet and race tests, with real PostgreSQL required.
- Redis driver race tests additionally passed with real standalone Redis 7 and
  a three-master Redis 7 Cluster required. Shared contract race tests passed
  with both real Redis and PostgreSQL required.
- The validation command's percentile convention has a deterministic unit test.
  Saved formal JSONL passed row-count, outcome, charged-row update and index
  validity checks; the independent reviewer also checked data and conclusions.
- Dedicated validation containers were removed after checks. Runtime source,
  production databases and the primary checkout were not modified.
