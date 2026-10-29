# Commercial SDK hardening contract

Scope: immediate and context-aware blocking admission for known positive costs.
This is not a scheduler, concurrency owner, billing ledger, refund mechanism or
output-token reservation system. See the [current contract](compatibility.md).

## Required contracts

- A normal quota denial debits no dimension. Redis computes/encodes before one
  final MSET; PostgreSQL uses one transaction. Confirmed pre-commit rejection or
  rollback is NotCharged. Lost replies and uncertain commits remain Unknown.
- Admission consumes capacity even when later business execution fails.
- Never blindly retry Unknown. There is no SDK result recovery after an uncertain
  mutation; a later NotCharged result cannot resolve earlier uncertainty.
- Allow returns Decision/error. Wait retries only
  valid capacity denials, stop on every error, preserve confirmed success and
  in-flight accounting, and add no worker, queue or fairness guarantee.
- Applications own clients, context deadlines, policy publication, trusted quota
  selection, observability callbacks and bounded physical cleanup.
- Admission does not store request identity. Only exact D1/P1 Redis state is
  accepted; unknown/malformed formats fail closed before mutation. Prune deletes
  only valid recovered D1 debt, never policy or unknown keys. No upgrade, migration
  or historical storage compatibility is guaranteed.
- Optional observation is synchronous, once per public admission call (one whole
  Wait), with no raw quota identifiers or replay field. Callbacks must be fast and
  concurrency-safe; panics propagate.
- Custom transport, proxy, hook and routing integrations must not retry ambiguous
  mutations. Redis NoRetry still permits safe routing and NOSCRIPT fallback.

## Delivery and deployment checks

Run formatting, module verification, vet, and real Redis/Cluster/PostgreSQL race
contracts. Preserve broad atomicity, contention, cancellation, state validation,
logical expiry, cleanup, rollback and commit-loss coverage. Validate direct driver
inputs and malformed responses, not only facade calls. Maintain matching English
and Chinese README contracts and runnable examples.

Historical [verification records](verification.md) do not validate the current
implementation. Target load, failover durability, TLS/ACL, clock stability, memory
headroom and backend topology require deployment-specific acceptance. A tag,
stable release, support SLA or production certification needs a separate decision.
