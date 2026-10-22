# Pacelock

Pacelock is a Go + Redis library for immediate, weighted, multidimensional GCRA admission.

- Keep the primary checkout on `dev`, clean; develop in `.worktrees/`.
- Define quota, failure, cancellation, and accounting contracts before changing behavior.
- Maintain matching English and Chinese READMEs. Separate shipped APIs from proposals.
- Do not add queues, workers, billing, or concurrency ownership to the core limiter.
- Use go-redis for Redis transport; justify additional runtime dependencies.
- Run gofmt, go test ./..., go vet ./..., and real-Redis integration tests with -race.
- Preserve unrelated changes; use Conventional Commits. No tags/releases without authorization.
