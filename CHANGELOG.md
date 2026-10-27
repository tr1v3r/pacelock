# Changelog

## Unreleased

### Breaking changes

- `New` and `NewWithOptions` now accept an atomic `Driver` rather than a Redis
  client. Construct the independent Redis or PostgreSQL driver explicitly.
- `Rule` now embeds the shared `Policy` type as `Rule{Name, Policy, Cost}` instead
  of flattening `Rate`/`Period`/`Burst` into top-level fields. Access and
  construct them as `rule.Policy.Rate` and `Policy{Rate: ..., Period: ...,
  Burst: ...}`; wire format, validation and behavior are unchanged.
- Redis examples and backend tests moved into `drivers/redis`; use
  `scripts/check.sh` to check every module. The core has no third-party dependencies.

### Added

- Redis single-MSET commit for all quota updates and complete admission results,
  v2 logical expiry, and scope-routed bounded cursor cleanup via `Driver.Prune`.
  Legacy quota/C1/P1 readers preserve existing debt and recovery boundaries.
  Upgrade requires drained old writers and scheduled cleanup; unconverted rollback
  and mixed writers are unsupported. Runtime admission now needs MSET permission.
- `Policy`, `Quota`, `Charge` and `Limiter.Bind` separate fixed policies from
  complete per-attempt charge sets across all drivers. `BoundLimiter` provides
  `Allow`, `AllowOnce` and scope-wide `Lookup`; charge order is normalized to
  binding order. This unreleased PR's API now constructs charges through the
  value-receiver `Quota.Charge(cost)`, capturing a quota snapshot and cost in private
  fields without validation or I/O. Bound admission validates the complete captured
  policy and requires matching names, effective microsecond-rounded intervals and
  bursts before dispatch (`ErrInvalid` / `NotCharged` on mismatch); equivalent valid
  policy expressions are accepted. `Quota.Name` / `Policy` remain public. Mutating
  a quota affects neither existing charges nor bindings; policy changes need a new
  binding. Existing rule-based APIs, Driver contracts and storage remain unchanged;
  no `AllowCharges` or `BoundLimiter.Wait` is added.

- Public `Driver`, `Admission`, `LookupRequest`, `Scope`, `Rule.Interval`, and a
  pure `Evaluate` transition for custom atomic persistence boundaries.
- PostgreSQL driver with explicit schema setup, domain row locking, backend time,
  transactional receipts/debits and bounded caller-scheduled cleanup.
- Shared backend contracts and real PostgreSQL rollback, lost-commit-reply recovery,
  lock-wait cancellation and expiry tests, plus a runnable pgx example.

- `AllowOnce` and `Lookup` for bounded idempotent admission and recovery, including
  conflict detection, cached rejection, immutable retention and pending markers
  protecting against repeat debit after interrupted writes.
- `Receipt`, `Error`, `OutcomeOf`, `ErrConflict`, `ErrOutcomeUnknown`, `ErrBackend`.
- `Validate`, `NewWithOptions` and optional synchronous `Observer` events.
- Redis Cluster, randomized token-bucket model, fuzz, malformed-state, lost-response
  and real ACL partial-write tests; runnable recovery example and benchmarks.
- Docker Compose fixtures, expanded CI, dependency/security checks and operating,
  compatibility, security, contribution and verification documents.
- Constructor retry-policy, Cluster post-execution lost-reply and minimum-ACL
  regression tests.

### Fixed

- Reject typed-nil clients, nil contexts and uninitialized limiters without panic.
- Validate complete script response invariants and stored policy bounds.
- Distinguish pre-write state errors from ambiguous transport/runtime failures.
- Assert model-test persistence and TTL from an atomic snapshot in the same script
  as admission; remove the missing-state exemption that could hide omitted writes.
- Mark concrete go-redis script commands with `NoRetry` to prevent ambiguous
  replay at node, Cluster and ASK pipeline layers while retaining safe redirects.
  Constructors do not perform network I/O or mutate client options.
- Align development installation instructions with `@dev`, classify `ErrState`
  recovery by `OutcomeOf`, and document Cluster's precise `+cluster|slots` ACL
  requirement and the distinction between quota writes and retained denials.

### Compatibility

- `Allow`, `AllowOnce`, `Lookup`, `Rule` and `Decision` retain their business semantics.
  The Redis v1 quota keys, Lua and retained-record encoding remain unchanged; the
  constructor is breaking. Redis and PostgreSQL do not share or migrate state.
- Concrete go-redis clients accept default, positive and disabled (`-1`)
  `MaxRetries` budgets. Other commands on a shared client keep their retry policy.
  Default/positive Cluster redirect budgets allow bounded MOVED/ASK recovery;
  explicit `MaxRedirects: -1` still disables it. Unsupported negative budgets are
  rejected. Custom Scripters, hooks, node factories and proxies must preserve the
  no-ambiguous-replay contract. Requires go-redis v9.22.0.
- Admission error strings changed; callers must use `errors.Is`/`errors.As`.
- `Allow` is still non-idempotent. New admission records do not retroactively
  deduplicate old calls. There is no automatic refund or output-token reservation.
- No tagged version or stability promise is created by this change.
