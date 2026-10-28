# Durable application admission example

This example explicitly opts into static caller-policy admission (`AllowStaticOnce` with `Rule`) for version-pinned policies and durable recovery. It is not the default dynamic quick start: publish `UpdatePolicies`, then submit name/cost `Charge` values for that path.

This example adds application-owned recovery around Pacelock. It uses only the
existing module dependencies. It is a single-host, single-attempt journal demo,
not an SDK execution API or a distributed transaction.

## Run against disposable Redis

From the repository root, start the disposable fixtures with `docker compose up
-d --wait`, then build `(cd driver/redis && go build -o /tmp/pacelock-durable ./examples/durable)`.
The default address is localhost:16379; override with `REDIS_ADDR`.
Choose fresh directories for these commands:

```sh
# Normal execution: the second run prints no upstream execution.
/tmp/pacelock-durable -dir /tmp/pace-normal new
/tmp/pacelock-durable -dir /tmp/pace-normal run
/tmp/pacelock-durable -dir /tmp/pace-normal run

# Redis completes, but the application discards the response (expected error).
/tmp/pacelock-durable -dir /tmp/pace-admission new
/tmp/pacelock-durable -dir /tmp/pace-admission -fault after-admission run
/tmp/pacelock-durable -dir /tmp/pace-admission recover
/tmp/pacelock-durable -dir /tmp/pace-admission status
/tmp/pacelock-durable -dir /tmp/pace-admission run

# Upstream runs, then completion is lost: recovery must refuse automatic retry.
/tmp/pacelock-durable -dir /tmp/pace-execution new
/tmp/pacelock-durable -dir /tmp/pace-execution -fault after-execution run
/tmp/pacelock-durable -dir /tmp/pace-execution recover
/tmp/pacelock-durable -dir /tmp/pace-execution run
```

Fault commands and the last two commands intentionally exit nonzero. Fault
injection is a controlled process exit after a boundary, not a real Redis outage.
The upstream is a console-printing mock. Run `docker compose down` afterward.
All examples share an account quota; repeated demos can legitimately be denied.

## Contract

| Journal state | Allowed next action |
|---|---|
| prepared | Save admitting, then send one AllowOnce |
| admitting | Lookup with a fresh deadline; never silently issue a new debit |
| admitted | Explicit run saves executing before invoking the mock |
| executing | Stop for manual upstream reconciliation |
| done / denied / not_charged | Terminal; repeated run has no side effects |

`recover` only observes admission; it never invokes upstream. A recovered allowed
result can be followed by an explicit `run` only because the exclusive durable
journal proves this example never entered executing. A receipt, Replayed flag or
Lookup result alone is insufficient. A failed or interrupted upstream leaves
executing, including a crash between saving intent and actually calling upstream.
This deliberately sacrifices automatic progress when execution is uncertain.
A real adapter must reconcile with its own business record/provider operation ID;
there is no exactly-once guarantee across a file, Redis and an upstream service.

Missing records, pending errors and expired recovery windows preserve uncertainty.
Denied IDs stay denied and their RetryAfter is historical, not a countdown.
Lookup does not extend retention. The example never deletes or refunds Redis
state, and never reuses an expired ID. A genuinely new business attempt uses `new`
in a new directory, only after the application has resolved prior execution intent.
`new` cannot overwrite an existing journal. Recovery is conservatively limited to
ten minutes from local creation, so clock synchronization matters.

## Storage and policy boundaries

An exclusive directory lock prevents concurrent processes using the same journal.
A real process kill leaves `lock/` behind. An operator must first prove the old
owner has stopped, then remove only the empty lock directory with `rmdir` and use
`recover`/`status`; never remove the journal or automatically steal a stale lock.
Each transition writes a temporary file, syncs it, renames it and syncs its parent
directory. This assumes a trusted local filesystem supporting those operations;
network filesystems, multiple hosts, lost disks, backups/rollback, untrusted paths
and unsupported directory-sync platforms need a different application store.
Do not edit/copy journals to create new attempts. Host crashes and real upstream
failures require deployment-specific validation beyond the injected exits.

Rules come from a versioned trusted builder in a fixed account/model/tenant order.
CLI tenant/tokens represent trusted inputs for this demo; a service must derive
tenant identity from authentication and cost from verified tokenization. Callers
cannot supply a partial rule list. Tests assert every required quota and cost.
Input cost is known; unknown output tokens, cached-token accounting and reservation/
reconciliation remain [design proposals](../../../../docs/design.md#后续设计llm-生命周期未实现).
GCRA does not reproduce arbitrary provider fixed/sliding windows.

Policy version 1 stays available for retained attempts. A real policy change must
coordinate all instances and active quota debt; changing an ID, quota name or
version alone is not a debt-preserving migration. The demo rejects unknown policy
versions and leaves such journals untouched for operator action.
