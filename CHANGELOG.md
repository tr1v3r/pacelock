# Changelog

## Unreleased

- Standard-library core with independent Redis and PostgreSQL drivers, shared
  atomic admission contracts, and explicit PostgreSQL schema setup via `Migrate`.
- Backend-authoritative dynamic admission is the default: publish versioned
  policies with `UpdateQuotas`, then call `Allow` / `AllowOnce` with names/costs.
  Rate 0 pauses fresh admissions; updates preserve absolute debt and retained replay.
- Dynamic `Limiter.Wait(ctx, charges...) error` copies charges once and resolves
  latest backend policies atomically on each ordinary attempt; `New` remains dynamic.
  Both dynamic and static Wait share a private loop, retry only valid capacity denials
  after advisory `RetryAfter`, and stop on every error, including pause, missing policy,
  invalid cost after a Burst reduction and transport errors even when `NotCharged`.
  Cancellation between attempts is `NotCharged`; in-flight uncertainty is preserved,
  and confirmed success stays success despite cancellation. Each call emits one
  `wait` or `wait_static` event. Attempts use an empty ID and zero retention: no
  receipts, automatic lookup, queues, workers, fairness or refunds. Policy updates
  do not proactively wake sleepers; a prior RetryAfter may delay use of relaxed policy.
- Standalone `NewStatic` / `NewStaticWithOptions` validate and copy fixed
  Revision-0 quotas locally, without a parent limiter or backend I/O.
  `StaticLimiter.Allow` / `AllowOnce` / `Wait` take public name/cost `Charge`
  values; `Lookup` shares scope-based receipt recovery. Omitted fixed quotas are
  neither checked nor charged; selected quotas are atomic within each call, not across separate calls.
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

Breaking rename: `Limiter.UpdatePolicies` and driver `UpdatePolicies` become
`UpdateQuotas`; `PolicyDriver` becomes `QuotaDriver`, and `PolicyUpdate` becomes
`QuotaUpdate`. The old public names are removed; update callers and custom drivers.
Update errors now report operation `update_quotas` instead of `update_policies`.
`Policy` and the pure `ApplyPolicies` helper keep their names. This is an API-only
rename: quota configuration changes never refill capacity or reset/rescale debt;
absolute TAT and retained receipts remain unchanged.

Breaking static API change: `BindStatic`, `StaticCharge`, `Quota.StaticCharge` and
rule-taking `Limiter.AllowStatic` / `AllowStaticOnce` / `WaitStatic` are removed.
Construct `NewStatic(driver, namespace, domain, quotas...)` (or
`NewStaticWithOptions(driver, namespace, domain, options, quotas...)`) and pass
`Charge{Name, Cost}` to its admission methods instead. `Rule`, `StaticAdmission`,
`StaticDriver` and the pure evaluation helpers remain available. `Wait` snapshots
charges, retries only ordinary denial, stops on every error and emits one observation;
a confirmed debit remains success even if cancellation races with the response.

Breaking change: static admission no longer rejects omitted quotas. Callers that
relied on complete-set enforcement must validate required quotas themselves or
submit them together explicitly. Full-set calls retain their existing ordering
and idempotency fingerprints.

The API is pre-v1. No stable release, automatic state conversion, cross-backend
failover, refund or upstream exactly-once execution guarantee is introduced.
