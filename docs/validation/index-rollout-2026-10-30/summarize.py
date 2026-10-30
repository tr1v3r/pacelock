#!/usr/bin/env python3
"""Summarize saved JSONL without running or modifying a database."""
import json
from pathlib import Path
from statistics import median

root = Path(__file__).resolve().parent
for filename in ("write-fillfactor100.jsonl", "write-fillfactor80.jsonl"):
    path = root / filename
    if not path.exists():
        continue
    records = [json.loads(line) for line in path.read_text().splitlines()]
    print(filename)
    for scopes in (1, 16):
        for indexed in (False, True):
            group = [r for r in records if r["kind"] == "write" and r["scopes"] == scopes and r["index"] == indexed]
            if not group:
                continue
            for r in group:
                assert r["errors"] == r["denied"] == 0
                assert r["updates_delta"] == r["allowed"] * r["dimensions"]
                assert r["before"]["ActualRows"] == r["after"]["ActualRows"] == r["rows"]
            print({"scopes": scopes, "index": indexed,
                   "ops/s": [round(r["ops_per_second"], 1) for r in group],
                   "p50_ms": [round(r["latency"]["P50ms"], 3) for r in group],
                   "p95_ms": [round(r["latency"]["P95ms"], 3) for r in group],
                   "p99_ms": [round(r["latency"]["P99ms"], 3) for r in group],
                   "wal_bytes/admit": [round(r["wal_bytes"] / r["allowed"], 1) for r in group],
                   "hot_percent": [round(100*r["hot_delta"]/r["updates_delta"], 1) for r in group],
                   "median_ops/s": round(median(r["ops_per_second"] for r in group), 1),
                   "after_sizes_bytes": [{k:r["after"][k] for k in ("TableBytes","PKBytes","RecoveryBytes")} for r in group]})
for filename in ("prune.jsonl", "ddl.jsonl"):
    path=root/filename
    if not path.exists():
        continue
    print(filename)
    for r in [json.loads(line) for line in path.read_text().splitlines()]:
        if r["kind"] in ("prune","ddl","ddl_uncontended"):
            print({k:v for k,v in r.items() if k not in ("storage", "note")})
