# Changelog

## Unreleased

- Standard-library core with independent Redis and PostgreSQL drivers, shared
  atomic admission contracts, and explicit PostgreSQL schema setup via `Migrate`.
- Fixed `Policy` / `Quota` bindings and snapshot-based `Quota.Charge(cost)` values;
  complete-set validation before dispatch. Rule-based admission and `Wait` remain
  available alongside `BoundLimiter`.
- Redis keys use `pacelock:{scope_hash}:`, quotas use `tat:interval:burst`, and
  receipts use `expires_us|fingerprint|decision_json`. Fingerprints encode
  `[retention_ms,step,burst,cost,name,...]` without an empty placeholder.
- One final Redis MSET atomically commits admitted quotas and complete receipts.
  Logical expiry is independent of caller-scheduled bounded `Driver.Prune` cleanup.
- Bounded idempotency retains both allow and deny decisions, detects parameter
  conflicts, and supports lookup without extending retention.
- Malformed receipts return `ErrState` / `Unknown`, never missing or a new
  admission. Lost replies and ambiguous commits retain unknown accounting outcomes.
- Per-command Redis `NoRetry` prevents ambiguous replay while preserving bounded
  MOVED/ASK redirects. Constructors perform no I/O or client option mutation.
- Synchronous observation, defensive validation, shared backend contracts, and
  model, concurrency, corruption, expiry, cleanup and fault-injection coverage.

The API is pre-v1. No stable release, automatic state conversion, cross-backend
failover, refund or upstream exactly-once execution guarantee is introduced.
