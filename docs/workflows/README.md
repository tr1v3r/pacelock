> Historical archive: the content below includes removed idempotent admission, receipt and lookup APIs. It is not current integration guidance or validation. Run historical commands only against their recorded source revision. Current dynamic/static admission exposes Allow/Wait, with no uncertain-result recovery; do not blindly retry Unknown. See the [current API](../../README.md).

# Admission workflows

> Historical snapshot: diagrams, measurements and API names below describe the pinned source revision, not the current API. Current static use constructs a standalone `NewStatic` / `NewStaticWithOptions`, then passes public `Charge{Name, Cost}` values to `StaticLimiter.Allow` / `Wait`. Default `New` returns dynamic `Limiter`. See the [current API](../../README.md#explicit-static-alternative-fixed-policies-and-costs); generated historical artifacts are intentionally unchanged.

[简体中文](README.zh-CN.md)

Three source-backed, interactive diagrams trace one admission attempt. Diagram
labels and explanation cards are in Simplified Chinese; Go APIs and protocol
names retain their original spelling.

| Diagram | What it traces | Editable source |
| --- | --- | --- |
| [Public admission](admission.html) | Bind → Charge → local validation → Driver → decision, replay or error | [JSON](admission.json) |
| [PostgreSQL](postgres.html) | Scope guard row lock → backend time → retained receipt → GCRA → quota/receipt writes → Commit | [JSON](postgres.json) |
| [Redis](redis.html) | Same-slot keys → Lua → backend time → retained receipt → GCRA → one final MSET → reply decoding | [JSON](redis.json) |

## Viewing

Clone the repository or download an HTML file, then open it in a browser. GitHub's
file view shows the HTML source, not the interactive viewer. No backend or build is
needed to view the checked-in HTML. The viewer supports zoom, light/dark themes,
source references and export.

The JSON files are the editable diagram sources. Each HTML file bundles its
viewer so readers do not need Archify installed.

## Scope and accounting

- All quotas are admitted and charged together, or a quota denial charges none.
  A retained denial can still write a receipt; PostgreSQL can also commit a newly
  inserted scope guard row. “Not charged” does not mean “no storage writes.”
- A matching retained receipt returns the original allow/deny decision without
  charging again, recalculating its wait, or extending retention. A replayed allow
  does not authorize executing the business operation again.
- `NotCharged` describes this invocation only. `Unknown` cannot exclude an earlier
  commit and must not trigger a blind retry of ordinary `Allow`.
- These diagrams cover a single attempt, not business execution, billing,
  refunds, queues or maintenance workers. Cards explain the separate `Wait`,
  `Lookup`, migration and explicit pruning paths.

## Source snapshot and maintenance

All three diagrams cite
[`6a8d58fc558fb8896ac9b8919c62f1bd7f3f8109`](https://github.com/tr1v3r/pacelock/tree/6a8d58fc558fb8896ac9b8919c62f1bd7f3f8109).
Source links remain pinned to that revision even if a file moves later. These are
reviewed snapshots, not automatically synchronized documentation. Re-trace the
implementation and update the JSON references when API, accounting, storage or
recovery contracts change; do not just replace the revision hash.

The Redis diagram's labels and cards describe the historical unversioned quota and
receipt formats, no Pending state, and role-aware pruning. Its pinned source links
predate this cleanup; use the [current Redis guide](../../driver/redis/README.md)
for the authoritative storage and error contract.

The checked-in HTML was generated with Archify 3.2.0. To regenerate from the
repository root with that version available, set `ARCHIFY_CLI` to the installed
`bin/archify.mjs` path and run:

```sh
review=".archify/workflow-review-$(date +%Y%m%d-%H%M%S)"
for name in admission postgres redis; do
  node "${ARCHIFY_CLI:?Set ARCHIFY_CLI to the installed Archify CLI}" finalize workflow \
    "docs/workflows/$name.json" "docs/workflows/$name.html" \
    --repo-root . --quality showcase --json --out-dir "$review/$name" || exit 1
done
```

`finalize` validates the specification and source references, generates the HTML,
checks the artifact and runs automated browser checks. Chrome must be available
for the browser gate. Use a fresh review directory after changing a candidate;
keep machine-local receipts, captures and review logs out of commits. The bundled
HTML is generated output: edit the JSON, not the viewer code inside the HTML.
