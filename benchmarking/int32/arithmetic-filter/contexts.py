#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Run unchanged benchmark binaries with/without the unused prices CSV preload."""
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
p.add_argument('--repeats',type=int,default=24)
p.add_argument('--iters',type=int,default=25)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()
if a.iters <= 0: p.error("iterations must be positive")
if a.repeats<12 or a.repeats%6:p.error('repeats must be a multiple of six, at least twelve')
work=Path('build-arithmetic-filter')/a.out.stem;work.mkdir(parents=True,exist_ok=True)
a.out.parent.mkdir(parents=True,exist_ok=True)
variants={side+'_'+context:(a.target if side=='target' else a.base,context) for context in ('prices','only') for side in ('base','target','replica')}
samples={v:{q:[] for q in ('filter_simple','filter_and','filter_arith','filter_or')} for v in variants};orders=[]
report={'notes':__doc__,'repeats':a.repeats,'iterations':a.iters,'warmups':3,'cpu_affinity':'2','cores':1,'samples_ms':samples,'orders':orders,
        'binaries':{s:{'path':str(b.resolve()),'sha256':hashlib.file_digest(b.open('rb'),'sha256').hexdigest()} for s,b in [('base',a.base),('target',a.target)]},
        'datasets':{n:hashlib.file_digest(Path('benchmarking/data/'+n+'.csv').open('rb'),'sha256').hexdigest() for n in ('prices','trades')}}
for repeat in range(a.repeats):
 order=list(variants);shift=repeat%6;order=order[shift:]+order[:shift]
 if repeat//6%2:order.reverse()
 orders.append(order)
 for variant in order:
  binary,context=variants[variant]
  env=dict(os.environ,IBEX_CORES='1',IBEX_COMPACT_INT32='0')
  for key in ('IBEX_PROFILE_OPERATORS','IBEX_ANSWERS_DIR','IBEX_DIGEST_OUT'):env.pop(key,None)
  command=['taskset','-c','2',str(binary.resolve()),'--suite','filter','--csv-trades',str(Path('benchmarking/data/trades.csv').resolve()),'--warmup','3','--iters',str(a.iters),'--no-include-parse']
  if context=='prices':command+=['--csv',str(Path('benchmarking/data/prices.csv').resolve())]
  run=subprocess.run(command,env=env,capture_output=True,text=True,check=True)
  (work/f'{repeat+1:02d}-{variant}.log').write_text(run.stdout+run.stderr)
  found={m[1]:float(m[2]) for m in re.finditer(r'bench (\w+):.*?avg_ms=([\d.]+),',run.stdout)}
  assert set(found)==set(samples[variant])
  for query,ms in found.items():samples[variant][query].append(ms)
 a.out.write_text(json.dumps(report,indent=2)+'\n');print(f'{repeat+1}/{a.repeats}',flush=True)
def compare(after,before):
 return {'median_paired_ratio':statistics.median(t/b for t,b in zip(after,before)),'p':wilcoxon_p([t-b for t,b in zip(after,before)]),'slower_pairs':sum(t>b for t,b in zip(after,before))}
results={}
for context in ('prices','only'):
 for query in samples['base_prices']:
  results[context+':'+query]={'median_ms':{s:statistics.median(samples[s+'_'+context][query]) for s in ('base','target','replica')},
                            'target_base':compare(samples['target_'+context][query],samples['base_'+context][query]),
                            'replica_base':compare(samples['replica_'+context][query],samples['base_'+context][query])}
for family in ('target_base','replica_base'):
 ordered=sorted(results,key=lambda k:results[k][family]['p']);running=0.
 for rank,key in enumerate(ordered):
  record=results[key][family];running=max(running,min(1.,record['p']*(len(ordered)-rank)));record['holm_p']=running
report['results']=results
a.out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(results,indent=2))
