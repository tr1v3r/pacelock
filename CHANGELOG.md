# Changelog

## Unreleased

### Breaking pre-v1 admission simplification

- Remove `AllowOnce`, `Lookup`, `Receipt`, `LookupRequest`, admission
  `ID`/`Retention`/`Fingerprint`, and `Event.Replayed`. Admission no longer retains
  results, detects admission-ID conflicts or provides uncertain-result recovery.
- `New` / `NewWithOptions` remain dynamic. `NewStatic` / `NewStaticWithOptions`
  construct independent fixed-policy limiters. Both expose
  `Allow(ctx, ...Charge) (Decision, error)` and `Wait(ctx, ...Charge) error`;
  only dynamic limiters expose `UpdateQuotas`.
- `Driver.Admit` and `StaticDriver.AdmitStatic` return `(Decision, error)`.
  `Admission` is Scope + Charges; `StaticAdmission` is Scope + Rules.
  Custom drivers and consumers must migrate together; no compatibility aliases.
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

### Other development-line API changes

- Backend-authoritative dynamic admission is the default; publish revisioned
  policies through `UpdateQuotas`, then submit names/costs. Rate 0 pauses admission;
  policy updates preserve absolute debt.
- Rename `UpdatePolicies` → `UpdateQuotas`, `PolicyDriver` → `QuotaDriver`,
  `PolicyUpdate` → `QuotaUpdate`. `Policy` and `ApplyPolicies` keep their names.
- Replace parent-bound static construction and rule-taking limiter methods with
  standalone static constructors and public `Charge{Name, Cost}` values. Static
  calls accept any nonempty subset; callers must select every applicable quota.
- Standard-library core, independent Redis/PostgreSQL modules, explicit Migrate,
  single-MSET Redis quota commit, logical expiry and application-scheduled cleanup.
- Per-command Redis NoRetry prevents ambiguous replay while preserving bounded
  MOVED/ASK redirects. Constructors perform no I/O or client option mutation.

No stable release, automatic state conversion, cross-backend failover or upstream
exactly-once execution guarantee is introduced. See [migration](docs/compatibility.md).
