#!/usr/bin/env python3
"""Run sequential smoke/capacity samples; no universal performance assertions."""
import argparse, json, pathlib, subprocess
p = argparse.ArgumentParser()
p.add_argument('--binary', required=True)
p.add_argument('--output', required=True)
p.add_argument('--addr', default='127.0.0.1:16379')
p.add_argument('--cluster', action='store_true')
p.add_argument('--duration', default='1s')
p.add_argument('--sustained', default='30s')
a = p.parse_args()
with pathlib.Path(a.output).open('w') as out:

    def run(mode, rules, domains, duration, warmup, workers):
        command = [a.binary, '-disposable', '-addr', a.addr, '-mode', mode, '-rules', str(rules), '-domains', str(domains), '-duration', duration, '-warmup', warmup, '-workers', str(workers)]
        if a.cluster:
            command += ['-cluster']
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        out.write(json.dumps(data) + '\n')
        out.flush()
        print(mode, rules, domains, round(data['throughput']), data['errors'], data['context_errors'], flush=True)
    for mode in ['allow', 'deny']:
        for rules in [1, 4, 32]:
            for domains in [1, 8]:
                run(mode, rules, domains, a.duration, '100ms', 8)
    run('allow', 32, 1, a.sustained, '1s', 16)
