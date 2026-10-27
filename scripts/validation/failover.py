#!/usr/bin/env python3
"""Isolated manual-promotion, OOM and synthetic-clock acceptance harness.
Build examples/recovery-probe first; Docker must be available. No host clock edits.
"""
import argparse, json, pathlib, subprocess, time, uuid
p = argparse.ArgumentParser()
p.add_argument('--probe', required=True)
p.add_argument('--image', default='redis:7-alpine')
p.add_argument('--output', required=True)
args = p.parse_args()
root = pathlib.Path(__file__).resolve().parents[2]
run = 'pace-validation-' + uuid.uuid4().hex[:10]
results = {'image': args.image, 'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'scenarios': {}}

def cmd(*v):
    return subprocess.check_output(list(v), text=True, stderr=subprocess.STDOUT).strip()

def docker(*v):
    return cmd('docker', *v)
results['image_id'] = docker('image', 'inspect', args.image, '--format', '{{.Id}}')

def redis(name, *v):
    return docker('exec', name, 'redis-cli', '--raw', *map(str, v))

def until(fn, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if fn():
                return
        except subprocess.CalledProcessError:
            pass
        time.sleep(0.1)
    raise RuntimeError('fixture readiness timeout')

def probe(port, domain, action):
    return json.loads(cmd(args.probe, '-addr', f'127.0.0.1:{port}', '-domain', domain, '-action', action))
for loss in (False, True):
    network = run + ('-loss' if loss else '-sync')
    primary = network + '-primary'
    replica = network + '-replica'
    try:
        docker('network', 'create', network)
        docker('run', '-d', '--name', primary, '--network', network, '-p', '127.0.0.1:17379:6379', args.image, 'redis-server', '--save', '', '--appendonly', 'no', '--maxmemory-policy', 'noeviction')
        docker('run', '-d', '--name', replica, '--network', network, '-p', '127.0.0.1:17380:6379', args.image, 'redis-server', '--save', '', '--appendonly', 'no', '--replicaof', primary, '6379')
        until(lambda: redis(primary, 'PING') == 'PONG' and 'master_link_status:up' in redis(replica, 'INFO', 'replication'))
        if loss:
            docker('network', 'disconnect', network, replica)
        results['redis_version'] = next((x.split(':', 1)[1] for x in redis(primary, 'INFO', 'server').splitlines() if x.startswith('redis_version:')))
        seed = probe(17379, network, 'seed')
        assert seed['allowed'] == 10, seed
        if not loss:
            # A fresh WAIT connection does not cover the probe's writes.
            # Compare the replica offset with the primary offset instead.
            offset = int([x.split(':')[1] for x in redis(primary, 'INFO', 'replication').splitlines() if x.startswith('master_repl_offset:')][0])
            until(lambda: int([x.split(':')[1] for x in redis(replica, 'INFO', 'replication').splitlines() if x.startswith('slave_repl_offset:')][0]) >= offset)
        before = time.monotonic()
        docker('kill', primary)
        if loss:
            docker('network', 'connect', network, replica)
        until(lambda: redis(replica, 'PING') == 'PONG')
        assert redis(replica, 'REPLICAOF', 'NO', 'ONE') == 'OK'
        until(lambda: 'role:master' in redis(replica, 'INFO', 'replication'))
        checked = probe(17380, network, 'check')
        recovery = time.monotonic() - before
        if not loss:
            assert checked['found'] == 10 and checked['allowed'] == 0, checked
        else:
            assert checked['missing'] == 10 and checked['allowed'] == 10, checked
        results['scenarios']['unreplicated_loss' if loss else 'replicated_promotion'] = {'seed': seed, 'after_promotion': checked, 'manual_recovery_seconds': recovery, 'extra_admissions_after_10_confirmed': checked['allowed']}
        if not loss:
            assert redis(replica, 'CONFIG', 'SET', 'maxmemory', '1') == 'OK'
            oom = probe(17380, network + '-oom', 'seed')
            assert oom['allowed'] == 0 and oom['errors'] == 10, oom
            assert redis(replica, 'CONFIG', 'SET', 'maxmemory', '0') == 'OK'
            results['scenarios']['noeviction_oom'] = oom
            source = (root / 'drivers/redis/internal/engine/gcra.lua').read_text()
            clock = 'local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])'
            now = int(time.time() * 1000000)

            def evaluate(ts):
                return redis(replica, 'EVAL', source.replace(clock, f'local now = {ts}'), 1, 'synthetic-clock', 0, '', 1000000, 1, 1, 'q')
            first = evaluate(now)
            forward = evaluate(now + 2000000)
            backward = evaluate(now - 86400000001)
            assert first.splitlines()[0] == '1' and forward.splitlines()[0] == '1' and ('PACE_STATE' in backward), (first, forward, backward)
            results['scenarios']['synthetic_clock'] = {'first': first, 'forward_2s': forward, 'backward_over_horizon': backward, 'scope': 'production Lua with substituted now, not host clock fault'}
    finally:
        for name in (primary, replica):
            subprocess.run(['docker', 'rm', '-f', '-v', name], capture_output=True)
        subprocess.run(['docker', 'network', 'rm', network], capture_output=True)
pathlib.Path(args.output).write_text(json.dumps(results, indent=2) + '\n')
print(json.dumps(results, indent=2))
