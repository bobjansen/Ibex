#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired native Column/hash-map width controls; no interpreter/Polars timing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ab_queries import wilcoxon_p

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--base',type=Path,default=Path('build-release/ibex-native-width64'))
p.add_argument('--target',type=Path,default=Path('build-release/ibex-native-width32'))
p.add_argument('--rows',nargs='+',type=int,default=[1000000,10000000])
p.add_argument('--groups',nargs='+',type=int,default=[1000,100000])
p.add_argument('--iters',type=int,default=12)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()
if a.iters<6 or a.iters%2:p.error('even iterations >=6 required')
report={'workload_sha256':hashlib.sha256(Path(__file__).with_name('native_width.cpp').read_bytes()).hexdigest(), 'iterations':a.iters,'affinity':sorted(os.sched_getaffinity(0)),
        'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'git_status':subprocess.check_output(['git','status','--short'],text=True),
        'binaries':{k:{'path':str(v),'sha256':hashlib.sha256(v.read_bytes()).hexdigest()}
                    for k,v in [('base',a.base),('target',a.target)]},'cases':[]}
for rows in a.rows:
 for groups in a.groups:
  for op in ('scan','filter','gather','group'):
   workers={}
   try:
    for name,binary in [('base',a.base),('target',a.target)]:
     proc=subprocess.Popen([str(binary),str(rows),str(groups),op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
     workers[name]=proc
     if not proc.stdout.readline().startswith('ready '):raise RuntimeError('worker initialization failed')
    samples={k:[] for k in workers}
    for iteration in range(-2,a.iters):
     checks=[]
     for name in (list(workers) if iteration%2==0 else list(reversed(workers))):
      proc=workers[name];proc.stdin.write('run\n');proc.stdin.flush()
      line=proc.stdout.readline().split()
      if len(line)!=2:raise RuntimeError(f'{name} failed during {op}: {line}')
      ms,check=line;checks.append(check)
      if iteration>=0:samples[name].append(float(ms))
     if checks[0]!=checks[1]:raise RuntimeError('width answers differ')
    before,after=samples['base'],samples['target']
    result={'rows':rows,'groups':groups,'operation':op,'samples_ms':samples,
            'median_ms':{k:statistics.median(v) for k,v in samples.items()},
            'int32_over_int64':statistics.median([t/b for t,b in zip(after,before)]),
            'p':wilcoxon_p([t-b for t,b in zip(after,before)]),'checksum':checks[0]}
    report['cases'].append(result)
    print(json.dumps({k:v for k,v in result.items() if k!='samples_ms'}),flush=True)
    a.out.parent.mkdir(parents=True,exist_ok=True);a.out.write_text(json.dumps(report,indent=2)+'\n')
   finally:
    for proc in workers.values():
     proc.stdin.close()
     if proc.wait(timeout=30)!=0:raise RuntimeError('worker failed correctness check')
