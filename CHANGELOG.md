# Changelog

## Unreleased

### Dynamic-only admission

- Add `Use(name, cost)` as shorthand for constructing a `Charge` for `Allow` and
  `Wait`, without policy binding or changes to admission validation.

- `New` / `NewWithOptions` construct the sole dynamic `Limiter`, exposing
  `Allow(ctx, ...Charge) (Decision, error)`, `Wait(ctx, ...Charge) error` and
  `UpdateQuotas`. Publish positive-revision quotas before requests.
- `Driver.Admit(ctx, Admission) (Decision, error)` receives Scope + Charges.
  `Rule`, `Validate` and `Evaluate` remain offline math helpers, not admission APIs.
- Admission is non-idempotent: no retained results, request identity, replay or
  uncertain-result recovery. Do not blindly retry Unknown; a mutation may have
  committed. No billing, refunds, business execution, queue or worker ownership.
- Backend policies are authoritative. Handle stale publication errors explicitly;
  restoring earlier parameters requires a higher revision. Updates preserve debt.
- Wait copies charges once, reads current backend policies per attempt, retries
  only valid capacity denials after RetryAfter and stops on every error. Preserve
  in-flight accounting certainty and confirmed success during cancellation; emit
  one observation for the complete Wait call.

### Exact Redis storage contract

- Accept only exact `D1|tat` debt and `P1|interval:burst:revision` policy formats.
  Marker `1` identifies the first dynamic storage format, independently of the
  positive, monotonically increasing `Policy.Revision`.
- Unknown, malformed or wrong-role stored formats fail closed before mutation.
  Equivalent same-revision policy publications remain read-only successes.
- Prune deletes only valid recovered D1 debt, never policy or unknown keys.
  Permanent policy revision fences and debt-preserving updates remain required.
- No upgrade, migration, mixed-version or historical storage compatibility is
  guaranteed. Historical experiments retain their original data and source IDs;
  they are not current implementation validation or compatibility promises.

### Maintenance and validation

- Remove duplicate Redis engine observation plumbing; validate driver decisions
  against the dispatched charge snapshot without allocating synthetic rules.
- Keep rollback, commit-loss, weighted multidimensional atomicity, contention,
  cancellation, state validation, cleanup and conservative Unknown coverage.
- Standard-library core, independent Redis/PostgreSQL modules, explicit PostgreSQL
  setup via `Migrate`, single-MSET Redis commit and application-scheduled cleanup.
- Per-command Redis NoRetry prevents ambiguous replay while preserving bounded
  MOVED/ASK redirects. Constructors perform no I/O or client option mutation.
- Keep current bilingual documentation, examples, security and contribution guidance
  aligned with the [current contract](docs/compatibility.md).

No stable release, cross-backend failover or upstream exactly-once guarantee is introduced.
