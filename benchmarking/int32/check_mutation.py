#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compile the actual compact tests against a corrupt producer, without editing sources."""
import json
from pathlib import Path
import shlex
import subprocess
import tempfile

root=Path.cwd()
build=root/'build-release'
entries=json.loads((build/'compile_commands.json').read_text())
with tempfile.TemporaryDirectory(prefix='ibex-compact-mutant-') as temp:
    temp=Path(temp)
    header=temp/'ibex/core/column.hpp';header.parent.mkdir(parents=True)
    source=(root/'include/ibex/core/column.hpp').read_text()
    needle='backing->values = std::move(values);'
    assert source.count(needle)==1
    source=source.replace(needle,'backing->values.assign(values.size(), 0);')
    source=source.replace('[[nodiscard]] static auto from_compact_int32',
                          '[[nodiscard, gnu::always_inline]] static auto from_compact_int32')
    header.write_text(source)
    # Reuse the exact original interpreter test body with a small parse helper.
    original=(root/'tests/test_interpreter.cpp').read_text()
    body=original[original.index('TEST_CASE("compact storage computes'):]
    unit=temp/'interpreter_compact.cpp'
    unit.write_text('''#include <ibex/parser/parser.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <catch2/catch_test_macros.hpp>
using namespace ibex;
static auto require_ir(const char* source) -> ir::NodePtr {
 auto parsed=parser::parse(source); REQUIRE(parsed.has_value());
 auto lowered=parser::lower(*parsed); REQUIRE(lowered.has_value());
 return std::move(*lowered);
}
'''+body)
    objects=[]
    sources=[root/'tests/test_column.cpp',root/'tests/test_gather_kernel.cpp',root/'tests/test_compact_int32.cpp',unit]
    for number,src in enumerate(sources):
        key='test_interpreter.cpp' if src==unit else src.name
        entry=next(e for e in entries if e['file'].endswith('/tests/'+key))
        args=shlex.split(entry['command'])
        args.insert(1,'-I'+str(temp))
        args[-1]=str(src)
        obj=temp/(str(number)+'.o');args[args.index('-o')+1]=str(obj)
        args=[arg for arg in args if arg not in ('-O3','-O2')]
        args.insert(1,'-O1')
        subprocess.run(args,check=True)
        objects.append(str(obj))
    binary=temp/'mutant-tests'
    catch=build/'_deps/catch2-build/src'
    libs=[build/f'src/{name}/libibex_{name}.a' for name in ('repl','parser','runtime','ir','core')]
    subprocess.run(['clang++',*objects,'-Wl,--start-group',*[str(f) for f in libs],'-Wl,--end-group',
                    str(catch/'libCatch2Main.a'),str(catch/'libCatch2.a'),'-ldl','-lmvec','-pthread','-o',str(binary)],check=True)
    result=subprocess.run([str(binary),'[compact-int32]~[ingestion]','--reporter','compact'],capture_output=True,text=True)
    print(result.stdout)
    if result.returncode==0 or '7 failed' not in result.stdout:
        raise AssertionError('the corrupt compact producer must fail all seven tests: '+result.stderr)
    print('All seven compact tests reject a corrupt producer; production source and binary untouched.')
