# Security policy

Pacelock is pre-v1. Security fixes target the current development line; there is
no promised LTS window or commercial response-time SLA. Dependency updates and
Go vulnerability scanning are configured in CI. A clean scan is not an audit.

Do not post credentials, live quota keys, tenant IDs, attack payloads or exploitable
vulnerability details in a public issue. Prefer GitHub's private vulnerability
reporting **if enabled** on this repository. If no private reporting route is
available, open a minimal issue requesting a private contact without disclosing
the vulnerability. This repository does not claim that private reporting has
already been enabled or promise an unconfigured contact address.

Include affected version/commit, Redis/Go versions, a minimal reproduction using
synthetic identifiers, expected impact and any suggested mitigation privately.

## Deployment boundary

Use authentication, TLS and private networking. Restrict keys to a dedicated
application prefix. For standalone Redis, admission and policy-update command grants are
`+evalsha +eval +get +mset +time`, plus the go-redis connection-handshake commands
that your configuration uses (for example `HELLO`, `AUTH`, `CLIENT`, `PING` and
`SELECT` when applicable).

For go-redis Cluster routing, also grant **`+cluster|slots`** on every node to allow
slot-map discovery and refresh. Quote `'+cluster|slots'` when passing it through a
shell so `|` is not treated as a pipe. Without it, the standalone grants are not
sufficient for Cluster. Do not grant all of `+cluster` just to allow slot discovery.
`COMMAND` is not required for Pacelock's current EVALSHA/EVAL routing path; only add
permissions for other commands if your integration actually requires them.

Admission and policy updates do not need `ACL`, `CONFIG`, `FLUSHALL`, `KEYS`,
`SCAN` or `DEL`.
Explicit cleanup additionally needs `+scan +del` and can use a separate identity.
SCAN has node-wide key-name visibility; use dedicated trusted maintenance access.
The SDK filters candidates by scope and rechecks expiry atomically. Other
administrative privileges belong only to isolated test/operations tooling.

Test the exact ACL in staging: script key permissions and commands invoked inside
scripts are both checked. Admission reads authoritative policies and debt before
one final MSET commits all accepted debt changes. Policy updates use a separate
atomic MSET and preserve debt. Missing/paused policies or malformed policy/debt
state fail closed; absent debt is valid for a new quota. An uncertain mutation is
not automatically retried. Keep noeviction,
schedule bounded cleanup, and monitor memory and maintenance lag. Cleanup can
remove only valid recovered D1 debt, never policy keys or unknown keys. Unknown,
malformed or wrong-role stored formats fail closed before mutation.

There is no retained-result or lookup API. Do not put secrets or unnecessary
personal data in quota names, error logs or diagnostic captures. Redis key hashing
is namespace isolation, not encryption, access control or anonymization. Restrict
access to backend data and backups. Do not use broad prefix deletion for cleanup.

The SDK cannot enforce a rule the caller omits, prevent untrusted callers from
choosing another domain, or compensate for external key deletion/eviction,
unauthorized state changes, clock jumps or replication loss. Validate identities
and assemble policies in trusted application code.
