# Contributing

Keep the primary checkout on `dev` and clean; develop in `.worktrees/`. Preserve
unrelated edits. Use Conventional Commits. No tag, release or publishing action
without explicit maintainer authorization.

## Changes

Define quota, accounting, cancellation, error and compatibility contracts before
changing behavior. Keep the English and Chinese READMEs aligned. Proposed APIs
must be clearly separated from shipped APIs. Keep queues, workers, billing and
concurrency ownership outside the core. Additional runtime dependencies require
an explicit justification; instrumentation uses a callback, not a telemetry SDK.

For correctness changes, add tests that exercise externally visible invariants:
no debit on denial, no replay debit, bounded retention and uncertain outcomes on
transport/runtime failures. A fake Redis alone is not adequate Lua verification.
Use deterministic seeds for model tests and preserve useful fuzz regressions.

## Required checks

```sh
gofmt -w .
go mod verify
sh scripts/check.sh
docker compose up -d --wait
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
PACELOCK_POSTGRES_DSN='postgres://pacelock:pacelock-test-only@localhost:15439/pacelock?sslmode=disable' \
PACELOCK_REQUIRED_DRIVER_BACKENDS=redis,postgres sh scripts/check.sh
(cd driver/redis/internal/engine && go test -run '^$' -fuzz FuzzInterval -fuzztime 20s .)
(cd driver/redis/internal/engine && go test -run '^$' -fuzz FuzzRecord -fuzztime 20s .)
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
docker compose down
```

The default integration suite includes `TestRedisConcurrency` on standalone Redis
and Cluster. CI repeats it with `-race -shuffle=on -count=3` and requires the
expected backends via `PACELOCK_REQUIRED_REDIS_BACKENDS`. See
[the concurrency matrix](docs/concurrency-testing.md) for invariants and reproduction.

Integration tests create and delete only their own namespaced keys. The cold-cache
contention test runs `SCRIPT FLUSH` on every test master (affecting the server-wide
script cache); use dedicated disposable fixtures, never a shared Redis service. ACL fault
tests also create/delete a temporary Redis user. The MSET OOM test temporarily
lowers maxmemory and restores it afterward; use only disposable fixtures. Use disposable test services,
never production. The Cluster fixture has three masters and no replicas; it
validates routing/atomicity, not failover availability or durability.

Record benchmark environment, commands and results for hot-path changes. Avoid
hard latency thresholds on shared CI runners. Go test coverage is evidence of
executed code, not proof of correctness or a production SLA.

## Release checklist (manual; does not authorize a release)

- Review `CHANGELOG.md`, both READMEs, API docs and compatibility notes.
- Confirm tests, minimum Go/Redis versions, Cluster, fuzz and security checks.
- Review quota state and admission record migration; never reset debt silently.
- Check examples compile and execute against disposable Redis.
- Review module dependency/license changes and `go mod verify`.
- Publish measured limitations, remaining deployment checks and migration steps.
- Obtain explicit authorization for version/tag/release, then verify resulting CI.
- Use semver and describe any pre-v1 breaking change; no stable API guarantee
  before an explicitly approved v1 release. Never rewrite a published tag.
