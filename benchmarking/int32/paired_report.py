#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Analyze retained git-comparison repeats as pairs, including an unchanged control.

The shell harness's IQR-based 'noise' label is not a paired significance test.
This report uses the project's Wilcoxon implementation and records raw samples.
"""
import argparse
import csv
import json
from pathlib import Path
import statistics
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ab_queries import wilcoxon_p

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('directory',type=Path)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()

def read(path):
    with path.open() as f:
        return {(r['framework'],r['query']):float(r['avg_ms']) for r in csv.DictReader(f,delimiter='\t')}
indices=sorted(int(f.stem.split('repeat')[1]) for f in a.directory.glob('base.repeat*.tsv'))
if not indices:raise SystemExit('no base repeat samples')
data={side:[read(a.directory/f'{side}.repeat{i}.tsv') for i in indices] for side in ('base','target','ctrl')}
keys=set(data['base'][0])
if any(set(row)!=keys for rows in data.values() for row in rows):raise SystemExit('query sets differ')
results=[]
for framework,query in sorted(keys):
    samples={side:[r[(framework,query)] for r in rows] for side,rows in data.items()}
    base,target,ctrl=(samples[side] for side in ('base','target','ctrl'))
    def comparison(after,before):
        return {'median_paired_ratio':statistics.median([t/b for t,b in zip(after,before)]),
                'wilcoxon_p':wilcoxon_p([t-b for t,b in zip(after,before)]),
                'slower_pairs':sum(t>b for t,b in zip(after,before))}
    results.append({'framework':framework,'query':query,'samples_ms':samples,
                    'candidate_base':comparison(target,base),'candidate_control':comparison(target,ctrl),
                    'control_base':comparison(ctrl,base)})
# Holm adjustment across the candidate/base comparisons in this report.
order=sorted(range(len(results)),key=lambda i:results[i]['candidate_base']['wilcoxon_p'])
running=0.0
for rank,i in enumerate(order):
    running=max(running,min(1.,results[i]['candidate_base']['wilcoxon_p']*(len(results)-rank)))
    results[i]['candidate_base']['holm_p']=running
report={'directory':str(a.directory),'iterations':len(indices),'cases':results,'notes':__doc__}
a.out.parent.mkdir(parents=True,exist_ok=True)
a.out.write_text(json.dumps(report,indent=2)+'\n')
for r in results:
    print(json.dumps({k:v for k,v in r.items() if k!='samples_ms'}))
