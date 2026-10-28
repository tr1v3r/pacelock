# Changelog

## Unreleased

- Standard-library core with independent Redis and PostgreSQL drivers, shared
  atomic admission contracts, and explicit PostgreSQL schema setup via `Migrate`.
- Backend-authoritative dynamic admission is the default: publish versioned
  policies with `UpdatePolicies`, then call `Allow` / `AllowOnce` with names/costs.
  Rate 0 pauses fresh admissions; updates preserve absolute debt and retained replay.
- Explicit static `BindStatic` / `StaticLimiter` bindings and snapshot-based
  `Quota.StaticCharge(cost)` values validate nonempty subsets before dispatch.
  Rule-based `AllowStatic` / `AllowStaticOnce` / `WaitStatic` remain available. Omitted bound quotas are neither checked nor
  charged; selected quotas are atomic within each call, not across separate calls.
  Idempotency fingerprints include only selected quotas in binding order; changing
  the subset with the same retained ID conflicts.
- Redis keys use `pacelock:{scope_hash}:`, static quotas use `tat:interval:burst`,
  dynamic debts use `D3|tat`, and permanent policies use `P3|interval:burst:revision`.
  Receipts use `expires_us|fingerprint|decision_json`. Static fingerprints encode
  `[retention_ms,step,burst,cost,name,...]` without an empty placeholder; dynamic
  fingerprints use a distinct discriminator and do not bind policy revisions.
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

Breaking change: bound admission no longer rejects omitted quotas. Callers that
relied on complete-set enforcement must validate required quotas themselves or
submit them together explicitly. Full-set calls retain their existing ordering
and idempotency fingerprints.

The API is pre-v1. No stable release, automatic state conversion, cross-backend
failover, refund or upstream exactly-once execution guarantee is introduced.
