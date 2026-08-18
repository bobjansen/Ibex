# Measuring and verifying changes in Ibex

A short guide for agents doing performance or correctness work here. Everything
below is a rule that was learned by getting it wrong, usually more than once.

## 0. The reporting rule

**Deliver a result, or say in one sentence that you did not.**

> "The runner terminated during the first 1-core pass before it wrote any result
> rows. The initial sandbox failed on the uv cache; the approved rerun got past
> that but was stopped externally, so there is no valid scaling report yet."

That is three sentences of narration around "I have no data." Write "No
measurement yet — the harness died twice, cause not yet diagnosed" and then go
diagnose it. A dead run is a bug to investigate, not an event that happened to
you. "Stopped externally" is not a finding; find out *what* stopped it.

The same applies to work: if you did not finish, say what is missing and why.
Never let a summary imply progress the tree does not contain.

## 1. Start with the smallest thing that discriminates

Do **not** open with the big harness. `benchmarking/aws/run-thread-scaling.sh`
and the 16M two-tier suite are deliberately **not run** as a routine step (see
`plans/README.md`); they take a long time, need AWS or a quiet box, and tell you
nothing you can act on until you already know which operator is slow.

The fast loop is seconds, not hours:

```bash
# write a synthetic case with ibex_eval, 1M rows is usually plenty
IBEX_PROFILE_OPERATORS=1 IBEX_CORES=8 \
  ./build-release/tools/ibex_eval --plugin-path build-release/tools case.ibex \
  2>&1 >/dev/null | grep 'op="aggregate'
```

`IBEX_PROFILE_OPERATORS=1` prints a per-node line (`build_self_ms`,
`next_self_ms`, `pool_work_ms`, `barriers`, `pool_tasks`) and a per-statement
`operator profile:` summary. That is where a 500ms serial block shows up, and it
costs one run.

Reach for the full suite only to confirm a change you already believe in, or to
check you did not regress something else.

## 2. Vary two dimensions, not one

A one-dimensional sweep will confidently tell you there is no bug.

A real example: `median(v) by {a,b}` cost 517ms at 9800 groups and 20ms at 5000.
Sweeping group count alone looked like a cliff at 6000. Sweeping key count alone
looked fine (one key handled 100k groups in 67ms). The actual trigger needed
**both** multiple keys and a particular hash-table size, and neither sweep could
see it. See `plans/parallelism-overview.md`.

When something looks like a threshold, ask what else changed at that threshold.

## 3. Form a hypothesis, then try to kill it cheaply

Being wrong fast is the job. In the case above, two plausible hypotheses were
measured and discarded before the real one:

* per-group heap indirection — flattened the slot array, changed nothing;
* group count itself — one key does 100k groups in 67ms.

Each cost one build. Write down the discarded ones; they stop the next person
repeating them.

## 4. Prove the code path you are testing actually runs

The single most common way to "verify" nothing. Two checks:

* **Mutation-test every test you add.** Break the code the test claims to cover
  (return an error, delete a term, `if (false)`) and confirm the test fails.
  If it still passes, the test is decorative. Restore immediately afterwards.
* **Check reachability.** The whole-table functions (`distinct_table`,
  `inner_join_table`, `aggregate_table`) run only for a subtree beneath a node
  the chunked builder declined, **within one statement**. A `let` materializes
  and breaks the chain, so

  ```
  let d = t[distinct { g, v }];
  d[select { m = median(v) }];      // does NOT reach distinct_table
  t[distinct { g, v }][select { m = median(v) }];   // does
  ```

  A test written the first way passes while covering nothing.

## 5. Anything that can change output gets a byte-identity check

The engine's contract is that parallel output equals serial output, byte for
byte. Before and after your change:

```bash
for q in benchmarking/tpch/queries/q??.ibex; do
  n=$(basename "$q" .ibex)
  IBEX_CORES=8 ./build-release/tools/ibex_eval \
    --plugin-path build-release/tools "$q" > out_new/$n.txt 2>/dev/null
done
diff -rq out_base out_new
```

Use `git stash` to build the baseline binary, and keep both binaries so you can
interleave. This applies to any edit inside an operator, not just "optimizations"
— a schema-carrier fix in the join needed it too.

## 6. Timing A/B: interleave, and respect the noise floor

Serial runs drift on this box. Run `base, target, base, target...` per query and
take medians. The per-query noise floor is about **±13%**, not ±2%. A 1–2% suite
total is a wash — say so rather than claiming a win.

Check the box is quiet first (`ps --sort=-pcpu -eo pcpu,comm | head`); WSL2
load average lies. Never run a build while benchmarking.

## 7. Build discipline

* **`-j 6` total across the whole box**, not per invocation. Two concurrent
  `cmake --build ... -j 4` calls is 8 compiles and has OOM-killed this machine.
* Build the specific target (`--target ibex_tests ibex_eval`), not everything.
* Before committing anything non-trivial, run the strict g++ leg —
  CI uses `-Wpedantic -Wconversion -Wshadow` with warnings-as-errors and local
  clang does not:
  `CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-gcc --target ibex_runtime`

## 8. Do not let `pgrep`/`pkill` match itself

`pkill -f ibex_eval` matches the shell running that very command and kills it.
`until ! pgrep -f "compare.sh"` never exits for the same reason. Use the bracket
trick:

```bash
pkill -f "[i]bex_eval"
```

This has cost two separate incidents, including one that looked exactly like a
harness "stopped externally".

## 9. When you change a constant, grep for its other copies

Hash mixers, magic seeds, gate predicates. A six-clause join gate was written out
identically in two files; a hash mixer had **three** copies and finalizing two of
them silently duplicated groups until a test caught it. Before editing one:

```bash
grep -rn "0x9e3779b97f4a7c15" src/ include/ libs/
```

Then extract the shared thing rather than updating each copy.

## 10. Trust a profiler number only after something independent agrees

This profiler has had five attribution bugs, all of which made "serial" look
bigger than it was. A figure that surprises you is a claim to check, not a
finding to report. Corroborate with wall-clock A/B before building a plan on it.
