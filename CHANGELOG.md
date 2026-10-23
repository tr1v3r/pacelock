# Changelog

## Unreleased

### Added

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

- Existing `New`, `Allow`, `Rule`, `Decision` and quota state/key encoding remain.
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
