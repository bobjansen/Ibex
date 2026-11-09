#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Profile unchanged executables, varying preceding filter execution only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from ab_queries import wilcoxon_p
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--base',type=Path,required=True)
p.add_argument('--target',type=Path,required=True)
p.add_argument('--repeats',type=int,default=12)
p.add_argument('--iters',type=int,default=150)
p.add_argument('--switch')
p.add_argument('--prefixes',nargs='+',choices=['keep','skip'],default=['keep','skip'])
p.add_argument('--events',default='cpu_core/cycles/u,cpu_core/instructions/u,cpu_core/branches/u,cpu_core/branch-misses/u,minor-faults')
p.add_argument('--extra-metrics',nargs='*',default=[])
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()
if a.iters <= 0: p.error("iterations must be positive")
if a.repeats<12 or a.repeats%6:p.error('repeats must be a multiple of six, at least twelve')
work=Path('build-arithmetic-filter')/a.out.stem;work.mkdir(parents=True,exist_ok=True)
a.out.parent.mkdir(parents=True,exist_ok=True)
variants={side+'_'+prefix:(a.target if side=='target' else a.base,prefix) for prefix in a.prefixes for side in ('base','target','replica')}
samples={v:[] for v in variants};orders=[]
report={'notes':__doc__,'repeats':a.repeats,'iterations':a.iters,'warmups':3,'prefix_iterations':25,'cpu_affinity':'2','cores':1,'switch':a.switch,'events':a.events,
        'binaries':{s:{'path':str(b.resolve()),'sha256':hashlib.file_digest(b.open('rb'),'sha256').hexdigest()} for s,b in [('base',a.base),('target',a.target)]},'samples':samples,'orders':orders}
for repeat in range(a.repeats):
 order=list(variants);shift=repeat%len(variants);order=order[shift:]+order[:shift]
 if repeat//len(variants)%2:order.reverse()
 orders.append(order)
 for variant in order:
  binary,prefix=variants[variant];path=(work/f'{repeat+1:02d}-{variant}.json').resolve()
  env=dict(os.environ,IBEX_CORES='1',IBEX_COMPACT_INT32='0',IBEX_GDB_PROFILE_OUT=str(path),IBEX_GDB_PREFIX=prefix,IBEX_GDB_EVENTS=a.events)
  if a.switch:env[a.switch]='1' if variant.startswith('target_') else '0'
  for key in ('IBEX_PROFILE_OPERATORS','IBEX_ANSWERS_DIR','IBEX_DIGEST_OUT'):env.pop(key,None)
  command=['taskset','-c','2','gdb','-q','-batch','-iex','set debuginfod enabled off','-x',str(Path(__file__).with_name('profile_unchanged.py').resolve()),'--args',str(binary.resolve()),'--suite','filter','--csv',str(Path('benchmarking/data/prices.csv').resolve()),'--csv-trades',str(Path('benchmarking/data/trades.csv').resolve()),'--warmup','3','--iters',str(a.iters),'--no-include-parse']
  run=subprocess.run(command,env=env,capture_output=True,text=True,check=False)
  path.with_suffix('.log').write_text(run.stdout+run.stderr)
  run.check_returncode()
  control=json.loads(path.read_text());assert control['profiled'] and control['iterations']==a.iters
  match=re.search(r'bench filter_arith:.*?avg_ms=([\d.]+),.*?rows=(\d+),',run.stdout)
  assert match and int(match[2])==2685013
  prefix_queries=re.findall(r'bench (filter_simple|filter_and): iters=(\d+),',run.stdout)
  assert prefix_queries==([('filter_simple','25'),('filter_and','25')] if prefix=='keep' else []),prefix_queries
  sample={'avg_ms':float(match[1]),'rows':int(match[2])}
  for line in path.with_suffix('.perf').read_text().splitlines():
   if not line or line.startswith('#'):continue
   parts=line.split(',');value,event=parts[0],parts[2]
   if event.startswith('cpu_core/'):
    event=event.split('/')[1]
   assert not value.startswith('<'),line
   sample[event]=float(value)/(a.iters+3)
  assert all(k in sample for k in ('cycles','instructions','branches','branch-misses','minor-faults'))
  samples[variant].append(sample)
 a.out.write_text(json.dumps(report,indent=2)+'\n');print(f'{repeat+1}/{a.repeats}',flush=True)
def compare(after,before):
 return {'median_paired_ratio':statistics.median(t/b for t,b in zip(after,before)),'p':wilcoxon_p([t-b for t,b in zip(after,before)]),'slower_pairs':sum(t>b for t,b in zip(after,before))}
results={}
for prefix in a.prefixes:
 for metric in ['avg_ms','cycles','instructions','branches','branch-misses']+a.extra_metrics:
  values={s:[r[metric] for r in samples[s+'_'+prefix]] for s in ('base','target','replica')}
  results[prefix+':'+metric]={'median':{s:statistics.median(v) for s,v in values.items()},'target_base':compare(values['target'],values['base']),'replica_base':compare(values['replica'],values['base'])}
for family in ('target_base','replica_base'):
 ordered=sorted(results,key=lambda k:results[k][family]['p']);running=0.
 for rank,key in enumerate(ordered):
  record=results[key][family];running=max(running,min(1.,record['p']*(len(ordered)-rank)));record['holm_p']=running
report['results']=results
a.out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(results,indent=2))
