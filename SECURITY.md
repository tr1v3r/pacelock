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
application prefix. Production command requirements are `EVALSHA`, `EVAL`, `GET`,
`SET` and `TIME` plus the go-redis connection-handshake commands your configuration
uses (for example `HELLO`, `AUTH`, `CLIENT`, `PING` and `SELECT` when applicable).
The normal runtime does not need `ACL`, `CONFIG`, `FLUSHALL`, `KEYS` or `SCAN`.
Those administrative privileges belong only to isolated test/operations tooling.

Test the exact ACL in staging: script key permissions and commands invoked inside
scripts are both checked. Redis failure after some writes can leave partial
charges. Keep noeviction and monitor memory. Quota names in retained result records
are plaintext; do not put secrets or unnecessary personal data in them. Redis key
hashing is namespace isolation, not encryption, access control or anonymization.

The SDK cannot enforce a rule the caller omits, prevent untrusted callers from
choosing another domain, or compensate for external key deletion/eviction,
unauthorized state changes, clock jumps or replication loss. Validate identities
and assemble policies in trusted application code.
