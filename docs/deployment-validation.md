# Deployment validation

These tools exercise **dedicated disposable deployments**, not production throughput,
durability or availability guarantees. Never run them against production or shared
Redis: integration tests alter ACLs, flush script caches and migrate slots; the
failover harness kills its own containers and deliberately exhausts memory.

## Reproduction

Use the Go version required by the repository and record its patch version. From
the repository root, run measurements sequentially:

```sh
docker compose -p pacelock-validation up -d --wait
(cd driver/redis && go build -o /tmp/pacelock-load ./examples/load)
python3 scripts/validation/load-matrix.py --binary /tmp/pacelock-load \
  --output /tmp/pacelock-load.jsonl --duration 10s --sustained 30m
python3 scripts/validation/load-matrix.py --binary /tmp/pacelock-load \
  --addr 127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002 --cluster \
  --output /tmp/pacelock-cluster.jsonl --duration 10s --sustained 30m
python3 scripts/validation/failover.py --output /tmp/pacelock-faults.json
PACELOCK_REDIS_ADDR=127.0.0.1:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002 \
PACELOCK_REQUIRED_REDIS_BACKENDS=standalone,cluster \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@127.0.0.1:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres sh scripts/check.sh
docker compose -p pacelock-validation down --volumes
```

The failover harness creates its own uniquely named primary/replica network and
cleans it up in `finally`. Loopback ports 17379/17380 must be free. It uses manual
promotion, no Sentinel, no persistence and no TLS. It executes the production
static-admission Lua directly, not the Go transport or an application recovery API.
Go transport, lost-response accounting and live ASK/MOVED coverage remain in the
integration suite.

## Load interpretation

The load tool uses `NewStatic` and `Allow` with fixed policies. The matrix separates
ordinary allow/deny, uses 1/4/32 dimensions and 1/8 domains, and finishes with a
sustained single-domain, 32-rule allowance workload. It no longer has receipt,
replay, lookup, retention or expiry-measurement modes.

- Configure `-rps`, `-workers`, `-domains` and `-rules` for the target workload.
  Saturation is closed-loop; offered rate is approximate and does not correct
  coordinated omission. This is not an end-user SLO test.
- Warmup is excluded from counters. Request failures, Unknown outcomes and context
  errors remain in measurements. In-flight calls drain under bounded contexts.
- Latency percentiles use a uniform reservoir capped at 100,000 samples, including
  failed operations. INFO snapshots capture backend CPU/memory/replication data.
- Logical debt recovery does not remove stored keys. Measure application-scheduled
  `Driver.Prune` cycles, cleanup lag and idle scopes separately. The tool deletes
  only its generated namespace after measurement.

## Fault contracts

The replicated-promotion scenario waits for the replica offset to cover ten
confirmed quota debits, kills the primary and promotes the replica. Fresh attempts
against those quotas must be denied. The unreplicated-loss scenario disconnects
replication before the debits; stale promotion deliberately demonstrates extra
capacity. There are no retained IDs or lookup-based recovery claims.

Noeviction OOM must return errors rather than ordinary denials. The synthetic-clock
case replaces only the production Lua time expression: a forward jump replenishes
capacity early and a large backward jump returns `PACE_STATE`. This is not a real
host-clock fault. Neither successful local promotion nor Cluster tests certify HA.

## Deployment acceptance

Define target load, duration, tail latency, tolerable lost debt/extra capacity,
recovery time and memory headroom before acceptance. Preserve command, source
revision, Go/Redis versions, host resources, network, persistence and replication
settings with raw results. Do not encode machine-specific throughput thresholds
in shared CI.

An Unknown admission may already have consumed capacity. Do not blindly retry or
reset quota keys; define a business-level degraded-mode/reconciliation policy.
Monitor memory, eviction, replication lag, pool saturation and backend errors.
Historical records in [validation](validation/) include removed receipt APIs and
old protocols; they are not measurements of the current implementation. Multi-host
TCP loss, persistence restart, real clock faults, production TLS/ACL and customer
workloads require separate acceptance.
