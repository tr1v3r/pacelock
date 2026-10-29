# Changelog

## Unreleased

### Breaking pre-v1 admission simplification

- Remove `AllowOnce`, `Lookup`, `Receipt`, `LookupRequest`, admission
  `ID`/`Retention`/`Fingerprint`, and `Event.Replayed`. Admission no longer retains
  results, detects admission-ID conflicts or provides uncertain-result recovery.
- `New` / `NewWithOptions` construct the sole dynamic `Limiter`, exposing
  `Allow(ctx, ...Charge) (Decision, error)`, `Wait(ctx, ...Charge) error` and
  `UpdateQuotas`. Publish positive-revision quotas explicitly before requests.
- Remove `StaticLimiter`, `NewStatic`, `NewStaticWithOptions`, `StaticDriver`,
  `StaticAdmission`, `ValidateStatic` and all backend static admission paths.
  `Rule`, `Validate` and `Evaluate` remain offline math helpers only.
- `Driver.Admit(ctx, Admission) (Decision, error)` remains unchanged; `Admission`
  is Scope + Charges. No compatibility aliases or deployment convenience APIs.
- Policies may live in code, but execution is backend-authoritative across rolling
  pods. Handle stale update rejection explicitly; rollback requires a higher revision.
  Retire old static writers before installing policies, preserving debt and receipts.
- Preserve dynamic Wait from dev #34: copy charges once, use current backend
  policies on each attempt, retry only valid capacity denials after RetryAfter,
  and stop on every error. Preserve in-flight accounting certainty and confirmed
  success during cancellation. Emit one observation for the complete Wait call.
- Preserve weighted multidimensional atomicity, policy revision fences, pause,
  debt preservation, explicit pruning, and conservative Unknown classification.
  Do not blindly retry Unknown: a mutation may have committed and there is no
  recovery API. No billing, refunds, business execution, queue or worker ownership.
- Legacy receipt storage is **not automatically dropped**. Existing Redis attempt
  keys and PostgreSQL admission tables/rows may remain. Retire old writers and
  coordinate explicit operator cleanup without resetting debt or policy fences.
- Migrate shared backend contracts, runnable examples and bilingual documentation;
  retain rollback, commit-loss, concurrency, cancellation and Unknown coverage.

### Redis dynamic format numbering

- Number the first dynamic storage format D1/P1: all new debt writes use D1;
  new policies and higher-revision publications use P1. Marker `1` is independent
  of the positive, monotonically increasing `Policy.Revision`.
- Keep strict read-only compatibility for D3/P3 and old tuple debt. Reads do not
  migrate state; equivalent same-revision updates remain no-write successes and
  leave P3 unchanged. The next allowed admission writes D1, preserving existing debt.
- Stop all old binaries, including maintenance, before marker migration: D3/P3-only
  readers cannot read D1/P1. No mixed old/new writers, resets, renames or deletions
  to migrate. Permanent revision fences and historical receipt data remain intact.
  See [migration](docs/compatibility.md#redis-state).

### Maintenance

- Align security/contribution guidance and error/prune comments with dynamic-only
  admission; remove the retired receipt fuzz command.
- Remove Redis engine observation plumbing duplicated by the public limiter and
  the unreachable legacy-receipt prune branch. Legacy receipts remain untouched.
- Validate driver decisions against the dispatched charge snapshot without
  allocating synthetic rules. Keep structural validation and conservative
  accounting for invalid driver responses.

### Other development-line API changes

- Backend-authoritative dynamic admission is the default; publish revisioned
  policies through `UpdateQuotas`, then submit names/costs. Rate 0 pauses admission;
  policy updates preserve absolute debt.
- Rename `UpdatePolicies` → `UpdateQuotas`, `PolicyDriver` → `QuotaDriver`,
  `PolicyUpdate` → `QuotaUpdate`. `Policy` and `ApplyPolicies` keep their names.
- Standard-library core, independent Redis/PostgreSQL modules, explicit Migrate,
  single-MSET Redis quota commit, logical expiry and application-scheduled cleanup.
- Per-command Redis NoRetry prevents ambiguous replay while preserving bounded
  MOVED/ASK redirects. Constructors perform no I/O or client option mutation.

No stable release, automatic state conversion, cross-backend failover or upstream
exactly-once execution guarantee is introduced. See [migration](docs/compatibility.md).
