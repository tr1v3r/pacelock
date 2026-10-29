> Historical archive: the content below includes removed idempotent admission, receipt and lookup APIs. It is not current integration guidance or validation. Run historical commands only against their recorded source revision. Current dynamic/static admission exposes Allow/Wait, with no uncertain-result recovery; do not blindly retry Unknown. See the [current API](../README.md).

> Historical verification below predates the Redis MSET/logical-expiry protocol.
> The current Redis suite replaces partial-write/Pending creation with atomic-commit
> ACL/OOM, logical-expiry, state validation and bounded cleanup tests. The historical
> [MSET local record](validation/redis-mset-local.md) predates removal of the Q2 quota
> prefix and receipt compatibility readers; neither report validates the current
> key prefix, quota/receipt encoding or fingerprint. Pending tests listed below
> belong only to the archived implementation, not the current suite.

# Verification record — 2026-10-23

Scope: commercial hardening of the known-cost admission core on
`feat/commercial-sdk`, based on `40ee465`. Results below are local executions, not
remote GitHub Actions results or a production certification. The primary `dev`
checkout was not modified. At the time of this local verification, no release,
tag or branch push had been performed; PR submission is a separate follow-up.

## Environment

- macOS/arm64, Apple M3 Pro (12 logical CPUs reported by Go benchmark).
- Go 1.24.13 for the compatibility floor; Go 1.27.3 for the final suite and security scan.
- Docker Engine 29.8.2; Redis 7.0.15, Redis 7.4.11 and Redis 8.10.2.
- Three Redis 7.4.11 masters in one container for Cluster routing, with no replicas.
- Standalone Redis and Cluster accessed over loopback ports from the host.
- Explicit isolated Go caches/toolchain selection; no change to the user's default Go.

The initial default Go installation had inconsistent binary/source versions.
Compatibility tests used an explicitly selected installed Go 1.24.13. A scan with
local Go 1.26.8 found standard-library advisories; an isolated official Go 1.27.3
was downloaded and the scan passed. See [compatibility.md](compatibility.md) for
security-maintained build requirements. The module's Go 1.24 floor does not imply
that old Go releases are safe production build toolchains.

## Completed checks

| Check | Result |
|---|---|
| `gofmt`, `git diff --check` | Passed |
| `go mod verify` | All modules verified; no new runtime dependency |
| `go test ./...` on Go 1.24.13 | Passed |
| `go vet ./...` on Go 1.24.13 and Go 1.27.3 | Passed |
| Real Redis 7.0.15 + Go 1.24.13, `-race -count=1` | Passed |
| Real Redis 7.4.11 + three-master Cluster, `-race -count=1` | Passed |
| Real Redis 8.10.2 + Go 1.27.3, `-race -count=1` | Passed |
| Final Go 1.27.3 suite against Compose Redis/Cluster | Passed; core package statement coverage **96.0%** |
| `FuzzInterval`, 10 seconds | Passed, 1,508,207 executions |
| `FuzzRecord`, 10 seconds | Passed, 1,835,506 executions |
| `govulncheck@v1.1.4 ./...`, Go 1.27.3 | No vulnerabilities found |
| YAML parsing, shell syntax, `docker compose config` | Passed |
| `docker compose up -d --wait` | Both standalone and Cluster healthy |
| `go run ./examples/llm` | Admitted against real Redis |
| `go run ./examples/idempotency` | Original allowed, second call replayed |

The examples are compiled by `go test ./...` and exercised separately. Their
`main` functions are not included in the **core package** coverage percentage.

Critical scenarios exercised:

- Quota denial does not charge any dimension; all blocking names are returned.
- Weighted contention across clients respects shared capacity.
- 100 concurrent callers of one retained ID produce one original admission.
- Replays/lookup do not extend TTL; denied IDs stay denied; expiry ends deduplication.
- Ordered names, effective policy, costs and retention are bound to the ID.
- A real admission followed by an injected lost response can be queried/replayed
  without another debit. This is client-result fault injection, not a WAN outage test.
- Redis ACL selectors allow writing the pending record and first quota, then deny
  the second quota write. Real script failure leaves a pending record, and retries
  do not charge the first quota again.
- Wrong key types, malformed state/replies and policy mismatches are rejected.
- 250 seeded cases compare the actual Lua code against an integer token-bucket
  oracle; only the clock expression is replaced for deterministic boundary checks.
- Cluster key-slot checks, routing across masters and cross-client same-ID contention.

## Local benchmark baseline

Command (run separately after fuzz/testing finished):

```sh
PACELOCK_REDIS_ADDR=127.0.0.1:16379 \
  go test -run '^$' -bench . -benchtime=300ms -benchmem .
```

Go 1.27.3, Redis 7.4.11, host-to-container loopback, serial calls, one benchmark
sample. [Raw output including allocations](benchmark-local.txt).

| Operation | 1 rule | 4 rules | 32 rules |
|---|---:|---:|---:|
| Allow, admitted | 141 µs | 173 µs | 301 µs |
| Allow, denied | 139 µs | 167 µs | 262 µs |
| AllowOnce, first admission | 145 µs | 182 µs | 343 µs |
| AllowOnce, replay | 155 µs | 146 µs | 207 µs |
| Lookup | 139 µs | 148 µs | 143 µs |

These are mean time/op measurements, not production throughput, p95/p99 latency,
a before/after optimization claim or an SLA. Docker networking, host scheduling,
policy count, retention volume and contention affect results. Use the supplied
benchmarks with the application's workload and deployment before capacity planning.

## Release/deployment gates that remain external

- The table above records local validation. Check the PR for remote results on
  the current head; the local record is not a substitute for those checks.
- Target deployment TLS/ACL, Redis failover, persistence, OOM, clock stability and
  sustained customer-load SLOs need environment-specific acceptance testing.
- Repository branch protection and private vulnerability reporting are hosting
  settings; this change documents them without changing account settings.
- Output-token reservation/reconciliation and provider accounting adapters remain
  explicitly unimplemented proposals, not advertised shipped functionality.
- A stable version, support SLA, tag or commercial launch requires an explicit
  maintainer decision. These cannot be established by passing local tests.

## PR security-tool compatibility follow-up

The initial GitHub Linux/amd64 security job exposed a parser panic in
`govulncheck v1.1.4` with Go 1.27.3 (`unexpected expr: *ast.KeyValueExpr`), despite
the successful local macOS scan. CI and contribution commands now pin
`govulncheck v1.9.0`. Running that tool with the Linux/amd64 analysis context and
Go 1.27.3 completed with **No vulnerabilities found**. This updates development
verification tooling only; the SDK runtime dependencies are unchanged.
