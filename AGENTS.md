# Pacelock

Pacelock provides immediate and context-aware blocking weighted, multidimensional GCRA admission through a standard-library core and independent Redis/PostgreSQL drivers.

- Keep the primary checkout on `dev`, clean; develop in `.worktrees/`.
- Define quota, failure, cancellation, and accounting contracts before changing behavior.
- Maintain matching English and Chinese READMEs. Separate shipped APIs from proposals.
- Do not add queues, workers, billing, or concurrency ownership to the core limiter.
- Use go-redis for Redis transport; justify additional runtime dependencies.
- Run gofmt and scripts/check.sh across every module; exercise real Redis/Cluster and PostgreSQL contracts with -race.
- Preserve unrelated changes; use Conventional Commits. No tags/releases without authorization.
