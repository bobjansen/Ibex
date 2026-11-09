#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Record x86 gather-loop positions and actual six-instruction byte hashes."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary',nargs=2,action='append',metavar=('NAME','PATH'),required=True)
p.add_argument('--out',type=Path,required=True)
a=p.parse_args()
report={}
for name,path in a.binary:
 binary=Path(path);payload=binary.read_bytes()
 sections=subprocess.check_output(['objdump','-h','-j','.text',str(binary)],text=True).splitlines()
 section=next(r.split() for r in sections if len(r.split())>5 and r.split()[1]=='.text')
 text_address=int(section[3],16);text_offset=int(section[5],16)
 functions=[]
 symbols=subprocess.check_output(['nm','-S','--defined-only','-C',str(binary)],text=True).splitlines()
 for row in symbols:
  if 'std::__do_visit' not in row or not re.search(r'kernel::(?:aligned_)?gather_selected(?:_values)?<(double|long|int)',row):continue
  address,size,_,symbol=row.split(maxsplit=3);address=int(address,16);size=int(size,16)
  asm=subprocess.check_output(['objdump','-d','--no-show-raw-insn',f'--start-address={address}',f'--stop-address={address+size}',str(binary)],text=True)
  ops=[]
  for line in asm.splitlines():
   m=re.match(r'\s*([a-f0-9]+):\s*(.*)',line)
   if m:ops.append((int(m[1],16),m[2]))
  loops=[]
  for i,(pc,op) in enumerate(ops):
   if not op.startswith('tzcnt'):continue
   body=ops[i:i+6];assert body[-1][1].startswith('jne'),body
   end=ops[i+6][0];begin_offset=text_offset+pc-text_address
   data=payload[begin_offset:begin_offset+end-pc]
   loops.append({'address':hex(pc),'mod32':pc%32,'mod64':pc%64,'bytes':len(data),'byte_sha256':hashlib.sha256(data).hexdigest(),
                 'ops':[x[1].split(' <')[0] for x in body]})
  functions.append({'symbol':symbol,'address':hex(address),'size':size,'loops':loops})
 report[name]={'binary':str(binary),'binary_sha256':hashlib.sha256(payload).hexdigest(),'functions':functions}
a.out.parent.mkdir(parents=True,exist_ok=True)
a.out.write_text(json.dumps(report,indent=2)+'\n')
for name,r in report.items():
 print(name)
 for f in r['functions']:
  print(f['symbol'].split('kernel::')[1][:60],[(x['mod32'],x['byte_sha256'][:12]) for x in f['loops']])
