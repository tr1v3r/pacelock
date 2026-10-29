# Deployment validation

This fixture explicitly opts into standalone static admission (`NewStatic`, then `StaticLimiter.Allow` / `AllowOnce` with name/cost `Charge` values) for pinned policies and reproducible recovery/load measurements. It is not the default dynamic quick start: publish `UpdateQuotas`, then submit name/cost `Charge` values for that path.

These tools measure a **dedicated disposable deployment**. They do not certify
production throughput, durability or availability. Do not run against production
or shared development Redis: tests change ACLs, flush script caches, migrate slots,
kill containers and deliberately exhaust memory. The load tool only deletes its
own generated namespace, but requires an explicit `-disposable` acknowledgement.

## Reproduction

Use the Go version required by the repository and record its patch version.
From the repository root:

```sh
docker compose -p pacelock-validation up -d --wait
(cd driver/redis && go build -o /tmp/pacelock-load ./examples/load)
(cd driver/redis && go build -o /tmp/pacelock-probe ./examples/recovery-probe)
python3 scripts/validation/load-matrix.py --binary /tmp/pacelock-load \
  --output /tmp/pacelock-load.jsonl --duration 10s --sustained 30m
# Run sequentially, not concurrently with other measurements.
python3 scripts/validation/load-matrix.py --binary /tmp/pacelock-load \
  --addr 127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002 --cluster \
  --output /tmp/pacelock-cluster.jsonl --duration 10s --sustained 30m
python3 scripts/validation/failover.py --probe /tmp/pacelock-probe \
  --output /tmp/pacelock-faults.json
PACELOCK_REDIS_ADDR=127.0.0.1:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002 \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster sh scripts/check.sh
docker compose -p pacelock-validation down
```

The failover harness creates its own uniquely named two-container network, binds
only loopback ports 17379/17380 and cleans up in `finally`. Those ports must be free.
It uses manual promotion, no Sentinel, no persistence and no TLS. Run multiple
samples on representative hardware. Configure the tool's `-rps`, `-workers`,
`-retention`, `-domains` and `-rules` for the target workload. A quota domain is one
slot; more independent domains can spread across Cluster masters, but on standalone
Redis they still share one server. The tool uses the existing conservative
no-redirect client settings; topology mutation tests are separate from load tests.

## Current Redis protocol interpretation

The archived October 24 results below used the old native-TTL/Pending protocol.
They do not measure the current MSET protocol or cleanup throughput. `-expiry`
waits for logical deadlines; current physical memory/key counts will not drain without
application-scheduled `Driver.Prune` cycles. Measure cleanup lag, per-scope scan
cost, idle scopes and prolonged maintenance failure as part of new acceptance.
Current ACL coverage checks all-or-none MSET rejection; malformed receipts return
`ErrState` / `Unknown` and cannot become a new admission.

## What the load results mean

- Six modes separate ordinary allow/deny, first idempotent allow/deny, replay and
  lookup. The matrix uses 1/4/32 dimensions and 1/8 domains. The final sample is a
  sustained single-domain, 32-rule idempotent workload.
- Saturation mode is closed-loop: each worker waits for its reply. `-rps` paces each
  worker and is an approximate offered aggregate rate, not an open-loop queue.
  Neither mode corrects coordinated omission; this is not an end-user SLO test.
- Warmup is excluded from counters and latency, but its live records remain in
  memory until expiry. Each phase uses different IDs. Results retain errors,
  Unknown outcomes and context cancellation/deadline counts; the phase deadline stops scheduling and in-flight calls drain under their own
  three-second deadline. Elapsed time includes that drain; request failures are
  not silently discarded. Investigate every non-context failure separately.
- Latency P50/P95/P99 uses a uniform reservoir capped at 100,000 samples, in
  microseconds, including failed operations. It is sampled, not an exact histogram.
  Counters and memory are exact observations for their measurement instants.
- INFO snapshots contain per-master CPU/memory/eviction/replication information
  for Cluster. Compare cumulative CPU counter deltas over the recorded interval.
  SCAN key counts are approximate while expiry is active. Used memory includes
  allocator, connections and server overhead; RSS need not fall when keys expire.
- `-expiry` waits retention plus two seconds and samples again. First-allow records
  and quota debt logically expire; physical removal requires cleanup. First-denial
  records expire but the deliberately
  exhausted 24-hour quota keys remain. Replay/lookup seeds deliberately have a
  24-hour retention, so the short retention wait is not an expiry assertion there.
- The tool captures Go build/dependency/VCS metadata and Redis version/config
  observations. Preserve the command, host/container resources, networking,
  commit/tree identity, persistence and replication configuration with the results.

## Fault contracts and observations

The harness first waits for the replica offset to cover admitted writes, kills
the primary, manually promotes the replica and counts retained IDs and extra
admissions. A second run disconnects the replication network **before** admissions,
then kills the primary and promotes the stale replica. It explicitly expects
record loss and additional capacity in the second scenario. `missing` never means
an earlier debit did not occur. Promotion time includes the harness commands and
probes, not automatic failover detection or an availability SLO.

The noeviction OOM case sets maxmemory below current usage on the isolated fixture:
admissions must return errors, never normal quota denials. The synthetic clock
case executes production Lua with only its `now` expression replaced: a forward
jump replenishes capacity early and a large backward jump returns PACE_STATE.
It is an algorithm boundary test, **not a host-clock chaos test**. The core suite exercises genuine MSET ACL rejection and malformed-receipt
recovery boundaries. The archived partial-write observation describes only the old protocol.
Live ASK/MOVED migration and lost-response accounting are covered by the Cluster
transport regression introduced with the redirect work; run it when that change
is present. Passing standalone/three-master tests alone does not establish HA.

## Acceptance before a deployment

Define the target load, duration, tail latency, maximum tolerable lost admissions,
extra capacity, recovery time and memory headroom **before** running acceptance.
Do not encode machine-specific throughput thresholds in shared CI. Archive raw
results even when limits fail. Follow up on Unknown outcomes without
resetting quota keys or reusing expired IDs. Set alarms for Redis memory, eviction,
replication lag, pool saturation and backend errors; define explicit degraded-mode
and business-reconciliation procedures.

Local records in [validation](validation/) demonstrate tools and bounded fixture
behavior. They do not replace multi-host TCP loss, hours-long stability, real
clock faults, persistence restart, production TLS/ACL, Sentinel/managed failover,
provider accounting or customer-workload acceptance. Stable releases and support
SLAs remain separate maintainer decisions.
