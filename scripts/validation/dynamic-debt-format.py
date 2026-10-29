#!/usr/bin/env python3
"""Historical bcc67d5 debt-format experiment; NOT a production scalar implementation.
Requires an isolated disposable Redis. Writes fixed bench:{d3}: keys; remove the
container after measurement. Reads pinned Lua from Git, never current source.
"""
import argparse, hashlib, socket, random, statistics, json, pathlib, subprocess
ROOT=pathlib.Path(__file__).resolve().parents[2]
REVISION='bcc67d523b67269e42faaacfa0767bf72020b996'
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--disposable', action='store_true', help='confirm isolated disposable Redis')
p.add_argument('--port', type=int, default=18379, help='loopback Redis port')
p.add_argument('--output', type=pathlib.Path, required=True, help='new JSON output file (not overwritten)')
p.add_argument('--rounds', type=int, default=21)
p.add_argument('--iterations', type=int, default=100000)
p.add_argument('--batch', type=int, default=2000)
options=p.parse_args()
if not options.disposable:
 p.error('requires --disposable; never use production or shared Redis')
if not (1 <= options.port <= 65535 and 1 <= options.rounds <= 100 and 1 <= options.iterations <= 1000000 and 1 <= options.batch <= 10000):
 p.error('invalid bounded benchmark parameters')
if options.output.exists() or not options.output.parent.is_dir():
 p.error('output must be a new file in an existing directory')
def source(name, expected):
 data=subprocess.check_output(['git','show',f'{REVISION}:driver/redis/internal/engine/{name}'],cwd=ROOT)
 if hashlib.sha256(data).hexdigest()!=expected:
  raise RuntimeError('historical source digest mismatch: '+name)
 return data.decode()
protocol=source('protocol.lua','15411a9c1f8a84dc09eeb9d85d1cf50ea28cebca7a357cc34393593111f889b0')
gcra=source('gcra.lua','51196c48d98c882deeb1552eb4dd461af33e3aad3c9897dd0d6bd57105528a5a')
class Redis:
 def __init__(self):
  self.s=socket.create_connection(('127.0.0.1',options.port),timeout=120); self.f=self.s.makefile('rb')
 def enc(self,args):
  a=[str(x).encode() if not isinstance(x,bytes) else x for x in args]
  return b'*'+str(len(a)).encode()+b'\r\n'+b''.join(b'$'+str(len(x)).encode()+b'\r\n'+x+b'\r\n' for x in a)
 def read(self):
  line=self.f.readline(); t=line[:1]; v=line[1:-2]
  if t==b'-': raise RuntimeError(v)
  if t==b':': return int(v)
  if t==b'+': return v.decode()
  if t==b'$':
   n=int(v)
   if n<0:return None
   out=self.f.read(n); self.f.read(2); return out.decode()
  if t==b'*':return [self.read() for _ in range(int(v))]
  raise RuntimeError(line)
 def cmd(self,*a):self.s.sendall(self.enc(a));return self.read()
 def batch(self,args,n):
  self.s.sendall(self.enc(args)*n)
  return [self.read() for _ in range(n)]
r=Redis();r.cmd('PING')
old="""    if string.sub(raw, 1, 3) == 'D3|' then
        local tat = tonumber(string.match(raw, '^D3|(%d+)$'))
        if not tat or tat > exact or tat > now + horizon then return nil end
        return {tat, 0, 0, tat <= now, true}
    end"""
new="""    local tat = tonumber(raw)
    if tat then
        if tat < 0 or tat ~= math.floor(tat) or tat > exact or tat > now + horizon then return nil end
        return {tat, 0, 0, tat <= now, true}
    end"""
assert protocol.count(old)==1
scalar=protocol.replace(old,new)
assert gcra.count("string.format('D3|%.0f', nexttat)")==1
sgcra=gcra.replace("string.format('D3|%.0f', nexttat)","string.format('%.0f', nexttat)")
rng=random.Random(20261029)
def stats():
 for line in r.cmd('INFO','commandstats').splitlines():
  if line.startswith('cmdstat_evalsha:'):
   d=dict(x.split('=') for x in line.split(':',1)[1].split(','));return int(d['calls']),int(d['usec'])
 return 0,0
def summary(samples):return dict(median=statistics.median(samples),min=min(samples),max=max(samples),samples=samples)
N=options.iterations; rounds=options.rounds; micro=[]
for mode in ['loop','d3_parse','scalar_parse','d3_reader','scalar_reader']:
 pre=scalar if mode=='scalar_reader' else protocol
 raw="'D3|'..string.format('%.0f',1700000001000000+i)" if mode.startswith('d3') else "string.format('%.0f',1700000001000000+i)"
 decode={'loop':'return 1700000001000000+index','d3_parse':"if string.sub(raw,1,3)~='D3|' then error('tag') end; return tonumber(string.match(raw,'^D3|(%d+)$'))",'scalar_parse':'return tonumber(raw)','d3_reader':'return quota_state(raw,now)[1]','scalar_reader':'return quota_state(raw,now)[1]'}[mode]
 script=pre+'\nlocal function decode(raw,now,index) '+decode+' end\nlocal values={}\nfor i=1,64 do values[i]='+raw+" end\nlocal sum=0\nfor i=1,tonumber(ARGV[1]) do local j=(i-1)%64+1;sum=sum+decode(values[j],1700000000000000,j)-1700000000000000 end\nreturn sum"
 micro.append(dict(mode=mode,sha=r.cmd('SCRIPT','LOAD',script),samples=[]))
for k in range(-3,rounds):
 rng.shuffle(micro)
 for c in micro:
  a,b=stats();result=r.cmd('EVALSHA',c['sha'],0,N);aa,bb=stats();assert aa-a==1
  assert result==sum(1000000+i%64+1 for i in range(N))
  if k>=0:c['samples'].append((bb-b)*1000/N)
full=[]
for n in [1,4,32]:
 for mode,p,g in [('d3',protocol,gcra),('scalar',scalar,sgcra)]:
  for outcome in ['allow','deny']:
   keys=[f'bench:{{d3}}:{mode:_<6}:{outcome}:{n}:q:{j}' for j in range(n)]; policies=[x+':policy' for x in keys]
   sha=r.cmd('SCRIPT','LOAD',p+'\nlocal managed=true\n'+g)
   args=['EVALSHA',sha,2*n]+keys+policies+[v for _ in range(n) for v in (0,0,1)]
   full.append(dict(mode=mode,n=n,outcome=outcome,keys=keys,policies=policies,args=args,samples=[]))
B=options.batch
for k in range(-3,rounds):
 rng.shuffle(full)
 for c in full:
  now=r.cmd('TIME'); now=int(now[0])*1000000+int(now[1])
  tat=now+60000000 if c['outcome']=='allow' else now+3600000000
  value=('D3|' if c['mode']=='d3' else '')+str(tat)
  policy='P3|1:1000000000:1' if c['outcome']=='allow' else 'P3|3600000000:1:1'
  r.cmd('MSET',*[x for key in c['keys'] for x in (key,value)],*[x for key in c['policies'] for x in (key,policy)])
  a,b=stats(); replies=r.batch(c['args'],B);aa,bb=stats();assert aa-a==B
  assert all(x[0]==(1 if c['outcome']=='allow' else 0) and len(x)==c['n']+2 for x in replies)
  if k>=0:c['samples'].append((bb-b)/B)
result=dict(redis=r.cmd('INFO','server'),rounds=rounds,micro_iterations=N,full_batch=B,micro_ns={c['mode']:summary(c['samples']) for c in micro},full_us={f"{c['outcome']}_{c['n']}_{c['mode']}":summary(c['samples']) for c in full})
result['source_revision']=REVISION
result['source_sha256']={name:hashlib.sha256(text.encode()).hexdigest() for name,text in [('protocol.lua',protocol),('gcra.lua',gcra)]}
path=options.output
with path.open('x') as output:
 output.write(json.dumps(result,indent=2)+'\n')
r.f.close();r.s.close()
print('MICRO ns/op')
for name,v in result['micro_ns'].items():print(name,round(v['median'],2))
print('FULL Redis server us/request (d3, scalar, delta, saving percent)')
for outcome in ['allow','deny']:
 for n in [1,4,32]:
  d=result['full_us'][f'{outcome}_{n}_d3']['median'];s=result['full_us'][f'{outcome}_{n}_scalar']['median']
  print(outcome,n,round(d,3),round(s,3),round(d-s,3),round(100*(d-s)/d,2))
print(path)
