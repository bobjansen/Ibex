# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""GDB script: isolate/profile integer abs in an unchanged ibex_bench executable.

Run with gdb -batch -x this_file --args ibex_bench --suite scalar ... .
IBEX_GDB_PREFIX=keep retains preceding scalar queries (25 iterations); skip omits them.
Breakpoints are outside the timed query iterations. perf attaches while the
inferior is stopped at run_benchmark entry, then stops at its return address.
Linux x86-64 SysV ABI and libstdc++ std::string layout are checked assumptions.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import gdb

def profile_query():
    out = Path(os.environ['IBEX_GDB_PROFILE_OUT']).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    prefix = os.environ.get('IBEX_GDB_PREFIX', 'keep')
    assert prefix in ('keep','skip')
    for command in ('set pagination off','set confirm off','set print thread-events off','set disable-randomization off'):
        gdb.execute(command)
    executable = gdb.current_progspace().filename
    symbols = subprocess.check_output(['nm','--defined-only',executable],text=True).splitlines()
    matching = [r.split()[-1] for r in symbols if len(r.split())==3 and r.split()[-1].startswith('_ZN12_GLOBAL__N_113run_benchmarkE')]
    assert len(matching)==1,matching
    entry = gdb.Breakpoint(matching[0], internal=True)
    record={'executable':executable,'prefix':prefix,'seen':[],'profiled':False,'prefix_output':[]}
    gdb.execute('run')
    while gdb.selected_inferior().pid:
        inferior = gdb.selected_inferior()
        assert 'i386:x86-64' in gdb.selected_frame().architecture().name()
        query = int(gdb.parse_and_eval('$rdi'))
        data = int.from_bytes(inferior.read_memory(query,8).tobytes(),'little')
        length = int.from_bytes(inferior.read_memory(query+8,8).tobytes(),'little')
        assert 0 < length < 100
        name = inferior.read_memory(data,length).tobytes().decode()
        record['seen'].append(name)
        if name != 'abs_int_price':
            if prefix == 'keep' and not record['profiled']:
                gdb.execute('set $rcx = 25')
                entry.enabled=False
                record['prefix_output'].append(gdb.execute('finish',to_string=True))
                entry.enabled=True
            else:
                gdb.execute('return (int)0',to_string=True)
        else:
            assert not record['profiled']
            iterations = int(gdb.parse_and_eval('$rcx'))
            warmups = int(gdb.parse_and_eval('$rdx'))
            stack = int(gdb.parse_and_eval('$rsp'))
            address = int.from_bytes(inferior.read_memory(stack,8).tobytes(),'little')
            returned = gdb.Breakpoint('*'+hex(address),temporary=True,internal=True)
            entry.enabled=False
            perf_command=['perf','record','-q','-e','cycles:u','-F','999','--call-graph','lbr','-o',str(out.with_suffix('.data')),'-p',str(inferior.pid)]
            with out.with_suffix('.perf-stderr').open('w') as stderr:
                perf=subprocess.Popen(perf_command,stdout=subprocess.DEVNULL,stderr=stderr)
                time.sleep(0.25)
                assert perf.poll() is None, 'perf failed to attach; see perf-stderr'
                gdb.execute('continue',to_string=True)
                perf.send_signal(signal.SIGINT)
                perf.wait(timeout=10)
            entry.enabled=True
            record.update(profiled=True,iterations=iterations,warmups=warmups,perf_command=perf_command)
            out.write_text(json.dumps(record,indent=2)+'\n')
            gdb.execute('detach',to_string=True)
            break
        gdb.execute('continue',to_string=True)
    assert record['profiled']
    out.write_text(json.dumps(record,indent=2)+'\n')

profile_query()
