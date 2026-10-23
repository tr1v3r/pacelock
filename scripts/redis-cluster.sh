#!/bin/sh
# Three Redis masters in one isolated container; integration fixture, not HA.
set -eu
pids=""
cleanup() { for pid in $pids; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM
for port in 17000 17001 17002; do
  redis-server --port "$port" --bind 0.0.0.0 --protected-mode no \
    --cluster-enabled yes --cluster-config-file "/tmp/nodes-$port.conf" \
    --cluster-announce-ip 127.0.0.1 --cluster-node-timeout 5000 \
    --save '' --appendonly no --maxmemory-policy noeviction &
  pids="$pids $!"
done
for port in 17000 17001 17002; do
  attempt=0
  until redis-cli -p "$port" ping >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 100 ]; then exit 1; fi
    sleep 0.1
  done
done
redis-cli --cluster create 127.0.0.1:17000 127.0.0.1:17001 127.0.0.1:17002 \
  --cluster-replicas 0 --cluster-yes
wait
