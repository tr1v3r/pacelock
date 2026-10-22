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
- Reject unsafe retry settings on concrete go-redis clients in `New` and
  `NewWithOptions`, locally with `ErrInvalid` and without network I/O or option
  mutation. Prevent Cluster's outer retry loop from silently replaying admissions.
- Align development installation instructions with `@dev`, classify `ErrState`
  recovery by `OutcomeOf`, and document Cluster's precise `+cluster|slots` ACL
  requirement and the distinction between quota writes and retained denials.

### Compatibility

- Existing `New`, `Allow`, `Rule`, `Decision` and quota state/key encoding remain.
- Pre-v1 safety tightening: previously accepted default `*redis.Client` instances
  now fail construction; explicitly configure `MaxRetries: -1`. Cluster clients
  require both `MaxRetries: -1` and `MaxRedirects: -1` (see normalized-option
  details in operations). Disabling the Cluster loop also disables transparent
  `MOVED`/`ASK` retries, trading availability during topology changes for protection
  against ambiguous replay. Custom Scripter wrappers, hooks, Cluster `NewClient`
  factories and proxies still require caller auditing.
- Admission error strings changed; callers must use `errors.Is`/`errors.As`.
- `Allow` is still non-idempotent. New admission records do not retroactively
  deduplicate old calls. There is no automatic refund or output-token reservation.
- No tagged version or stability promise is created by this change.
