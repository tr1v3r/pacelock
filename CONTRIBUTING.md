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
gofmt -w *.go examples/*/*.go
go mod verify
go test ./...
go vet ./...
docker compose up -d --wait
PACELOCK_REDIS_ADDR=localhost:16379 \
PACELOCK_REDIS_CLUSTER_ADDRS=localhost:17000,localhost:17001,localhost:17002 \
  go test -race -count=1 -cover ./...
go test -run '^$' -fuzz FuzzInterval -fuzztime 20s .
go test -run '^$' -fuzz FuzzRecord -fuzztime 20s .
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
docker compose down
```

Integration tests create and delete only their own namespaced keys. ACL fault
tests also create/delete a temporary Redis user. Use disposable test services,
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
