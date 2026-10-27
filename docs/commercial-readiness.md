# Commercial SDK hardening contract

Redis-specific hardening details below remain applicable to the Redis driver.
For PostgreSQL deployment limits, see [its guide](../drivers/postgres/README.md).

Scope: the existing immediate, known-cost, weighted multidimensional limiter.
This work does not turn the limiter into a scheduler, concurrency owner, billing
ledger, or output-token reservation system. Existing `New`, `Allow`, `Rule`,
`Decision`, and quota key encoding remain compatible.

## Contracts fixed before implementation

- A normal quota denial changes no quota. Redis computes/encodes before one
  final MSET of quotas and receipt. Script-visible commit rejection is NotCharged;
  lost replies/crash durability still require conservative recovery.
- Admission consumes capacity for a business attempt, even if later work fails.
  There is no implicit refund, queue, background worker, or automatic retry.
- Invalid inputs and pre-dispatch cancellation do not charge. Errors expose
  whether the admission outcome is unknown; callers can still use errors.Is/As.
- `AllowOnce(ctx, id, retention, rules...)` caches both successful and denied
  decisions in the same slot as quotas. An ID identifies one admission, not an
  entire business request across multiple upstream attempts. Retention is an
  explicit whole-millisecond duration between 1 ms and 24 h. It is not extended
  by reads or replays. Rule order is significant, and effective policy, costs,
  names and retention are bound to the ID. A mismatch never charges.
- New Redis operations commit complete results without Pending. Legacy Pending
  remains unknown and cannot be replayed into another debit while retained.
  Logical expiry is independent of bounded application-scheduled cleanup.
  Only a complete record can be replayed or returned by `Lookup`. Missing records
  do not prove that an admission never happened. Expiry, eviction, deletion and
  failover data loss bound all deduplication guarantees.
- Replayed decisions describe the original observation. In particular, a denied
  ID stays denied until expiry, and its RetryAfter is not a fresh reservation or
  countdown. A genuinely new attempt needs a new ID.
- `Allow` remains non-idempotent. No method performs transport retries. Client,
  proxy and Cluster redirection behavior remains an integration responsibility.
- Optional observation runs synchronously once per public operation, outside
  Redis execution, with no SDK goroutines and no raw quota/attempt identifiers.
  Callbacks must be concurrency-safe and fast; callback panics propagate.

## Delivery checks

- [x] Defensive inputs, complete reply/state validation, typed outcome errors
- [x] Bounded idempotency, conflict detection, pending protection, lookup
- [x] Optional dependency-free observation and runnable examples
- [x] Model, fuzz, contention, replay, cancellation and lost-response tests
- [x] Redis Cluster integration and reproducible local/CI infrastructure
- [x] Benchmarks with environment and scope, not unsupported throughput claims
- [x] Matching English/Chinese READMEs and operational/compatibility guidance
- [x] gofmt, go test ./..., go vet ./..., real Redis tests with -race

## Release boundary

No tag, release, publish, push or stable-version claim is part of this change.
Production failover durability, customer workload SLOs and provider-specific
accounting need deployment validation. Reservation/reconciliation remains a
separate proposal with conservation and recovery contracts to define first.

Verification results and deployment gates: [verification.md](verification.md).
