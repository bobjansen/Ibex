#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Byte-identity checks for native compact Parquet decoding and Int64 results."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,default=Path.cwd())
p.add_argument('--reference-root',type=Path)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()
records=[]
with tempfile.TemporaryDirectory(prefix='ibex-compact-parquet-') as temp:
    temp=Path(temp)
    pq.write_table(pa.table({'v':pa.array([2000000000,2000000000,None],type=pa.int32()),
       'g':pa.array([1,1,2],type=pa.int32()),
       'w':pa.array([9223372034854775807]*3,type=pa.int64())}),temp/'boundary.parquet')
    pq.write_table(pa.table({'v':pa.array([-2147483648,2147483647],type=pa.int32())}),temp/'negative.parquet')
    n=1000000
    pq.write_table(pa.table({'v':pa.array(np.arange(n,dtype=np.int32)%2001-1000),
                            'g':pa.array(np.arange(n,dtype=np.int32)%17)}),temp/'sharded.parquet',row_group_size=65536)
    cases=[('sum','boundary','t[select { s=sum(v), p=sum(v*v), n=count(v) }];'),
           ('group','boundary','t[select { s=sum(v), p=sum(v*v), n=count(v) }, by {g}][order {g}];'),
           ('mixed','boundary','t[select { x=v+w }];'),
           ('negative','negative','t[select { n=-v, a=abs(v), d=v-1 }];'),
           ('sharded','sharded','t[select { s=sum(v), p=sum(v*v), n=count() }];'),
           ('selected','sharded','t[filter v>=0][select { s=sum(v), p=sum(v*v), n=count() }];')]
    for name,fixture,query in cases:
        script=temp/(name+'.ibex')
        script.write_text(f'import "parquet";\nlet t=parquet::read("{temp/fixture}.parquet");\n{query}\n')
        outputs=[]
        routes=[(a.root,'0'),(a.root,'1')]
        if a.reference_root:routes.insert(0,(a.reference_root,'0'))
        for cores in ('1','8'):
            for root,mode in routes:
                r=subprocess.run([str(root/'build-release/tools/ibex_eval'),'--plugin-path',str(root/'build-release/tools'),str(script)],
                    env=dict(os.environ,IBEX_CORES=cores,IBEX_COMPACT_INT32=mode),capture_output=True,text=True)
                if r.returncode:raise RuntimeError((name,cores,mode,r.stderr))
                outputs.append(r.stdout)
        if len(set(outputs))!=1:raise AssertionError((name,outputs))
        records.append({'case':name,'byte_identical':True,'stdout':outputs[-1]})
        print(name+': byte-identical, 1/8 threads, compact on/off'+(' and reference' if a.reference_root else ''))
a.out.parent.mkdir(parents=True,exist_ok=True)
a.out.write_text(json.dumps(records,indent=2)+'\n')
