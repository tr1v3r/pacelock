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

### Fixed

- Reject typed-nil clients, nil contexts and uninitialized limiters without panic.
- Validate complete script response invariants and stored policy bounds.
- Distinguish pre-write state errors from ambiguous transport/runtime failures.

### Compatibility

- Existing `New`, `Allow`, `Rule`, `Decision` and quota state/key encoding remain.
- Admission error strings changed; callers must use `errors.Is`/`errors.As`.
- `Allow` is still non-idempotent. New admission records do not retroactively
  deduplicate old calls. There is no automatic refund or output-token reservation.
- No tagged version or stability promise is created by this change.
