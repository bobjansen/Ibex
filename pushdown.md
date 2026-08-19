# Query Planner Pushdown — Q02/Q04 Regression, Root-Caused and Fixed

## Numbers (SF2, `taskset -c 0-7`, `IBEX_CORES=8`, median of paired repeats via `ab_queries.py`)

| query | before | after  | manual-pushdown query (pre-`eb5231c`) | speedup |
|-------|-------:|-------:|---------------------------------------:|--------:|
| q02   | 150.0ms|  57.2ms| 60.3ms                                 | 2.6×    |
| q04   | 385.2ms|  97.4ms| 97.7ms                                 | 3.95×   |
| q21   |1049.3ms| 623.0ms| —                                       | 1.7×    |
| q22   |  87.5ms|  80.9ms| —                                       | 1.08×   |

q02/q04 now match the hand-written, manually-pushed-down query from before
commit `eb5231c` ("Match query shapes") to within noise — the automatic
planner now does the same job the manual `select {}`/`filter` placement in
the old query files was doing by hand. q21/q22 were not part of the original
complaint but improved as a side effect of the same fix (both use semi/anti
joins). Full 22-query suite: byte-identical output before/after; the other
18 queries are unchanged (`same`/`unclear`, nothing outside noise once
re-run at higher `--repeats`; q05's initial +3.8% read was noise — it
settles to -0.4%/`same` at 20 repeats).

Two independent bugs were causing this, found by comparing the current
auto-planned q02.ibex/q04.ibex against their pre-`eb5231c` hand-optimized
versions column-by-column and join-by-join — **not by guessing**. The old
query files did three things by hand that the planner now does
automatically for the same effect: filtered the base table before joining,
projected every join operand down to only the columns needed, and renamed
join keys to a shared name. That manual work is why the old queries never
needed the planner's cost model to get this right — worth remembering before
crediting an unrelated fix (see the retracted Bug #1 below).

## Bug #1 (real fix, but NOT what was making q02/q04 slow — see retraction)

`src/ir/cardinality.cpp`'s Filter-family cardinality estimate applied
`filter_selectivity` (default 0.25) once regardless of AND-conjunct count. A
2-conjunct filter (true selectivity ≈ 1/250) was estimated 60× too high,
which matters for `join_reorder.cpp`'s `reorder_inner_joins_for_aggregates`
(wired only in `repl.cpp`, not `lower.cpp`). Fixed by raising selectivity to
the conjunct count (independence assumption). Real, valuable, covered by
`[cardinality]`/`[join_reorder]` tests — but **`ab_queries.py --base
eval_base --target eval_current` (cardinality fix only, no other change)
showed q02 unchanged (148.0ms → 144.1ms, well within noise)**. It doesn't
reproduce on the actual files because neither `european_offers` nor the
`orders semi join lineitem` chain sits directly under an `Aggregate`. Kept
because it's independently correct, not because it explains the regression.

## Bug #2 (the real q02 cost): shared bindings lose column pruning

Isolated with per-column decode timing (`LazyTable::decode_columns`
instrumented temporarily) on the **actual** `q02.ibex`, `taskset -c 0-7
IBEX_CORES=1`:

```
cols=p_partkey,p_name,p_mfgr,p_brand,p_container,p_retailprice,p_comment  selection=1  13.35ms
cols=ps_suppkey,ps_availqty,ps_supplycost,ps_comment                     selection=1  74.19ms
```

vs. the pre-`eb5231c` query, same table, same selected row count:

```
cols=p_partkey,p_mfgr           selection=1   2.99ms
cols=ps_suppkey,ps_supplycost   selection=1  13.95ms
```

`european_offers` is a "shared binding" (`src/parser/lower.cpp`'s
`share_repeated_bindings_`: referenced ≥2 times, contains a join, so it's
materialized once instead of re-run per reference). It's evaluated on its
own by `evaluate()` in `try_execute_whole_script` (`src/repl/repl.cpp`,
which is what `ibex_eval` actually runs — `tools/ibex_eval.cpp` calls
`repl::execute_script`, not `parser::lower()`/`lower_script()` directly).
`ir::required_columns()`, called on the shared binding's plan **in
isolation**, has no consumer above the plan's root to prove a narrower
demand from, so it must assume every column is wanted — decoding all of
`part`'s and `partsupp`'s payload columns, including the large VARCHAR
`p_comment`/`ps_comment` fields nothing downstream ever reads. `ps_comment`
alone accounted for ~58ms of the ~90ms real-world gap.

**Fix** (`src/repl/repl.cpp`, before the shared-bindings evaluation loop):
compute the union of `ir::required_columns()` demand for each shared
binding's name across every plan that can actually reference it — the
result plan, every sink's input, and every other shared binding's plan (a
later one may scan an earlier one, per the existing evaluation-order
comment) — all of which are already fully lowered and available at this
point since the whole script is planned together. When that union comes out
bounded (not `.all`), pin it to the top of the binding's own plan with a
synthetic `Project`, so its own `evaluate()` call only ever decodes what's
actually read downstream.

**Correctness trap this needed a guard against, found by the test suite**
(q15/q17/q21/q22 broke on the first version of this fix): `required_columns`
is a conservative *over*-approximation built for a different job — deciding
what a base-table scan may skip, where wanting one column too many costs
nothing. A name it can't trace the provenance of past an Aggregate/Project
boundary (q15's `max_revenue`, computed by a *sibling* subquery over the
same `revenue` binding, not read from `revenue` itself) gets attributed to
every visible source as the safe direction for *that* job. Reusing the
demand set as an exact projection list is the opposite safe direction, so
it isn't trusted blind: the fix intersects the demand against the binding's
own statically-known, closed output schema (`ir::infer_schema` — Ibex is
typed at read time, so this schema is proven, not guessed) and keeps only
names the plan can actually produce. An unprovable schema (not `is_known()`,
or `is_open()`) means declining the narrowing for that binding rather than
acting on an incomplete one. The real fix here is `required_columns`
learning to track provenance through Aggregate/Project boundaries the way
`infer_schema`/`check_column_refs` already do — the intersection guard is a
correct belt-and-suspenders workaround, not that fix.

## Bug #3 (the real q04/q21/q22 cost): Semi/Anti joins never pushed a filter down

`src/ir/join_pushdown.cpp`'s `rewrite_filter_over_join` explicitly declined
to rewrite `Semi`/`Anti` joins at all (`kind != Inner && kind != Left &&
kind != Right → return node`). q04's `orders semi join
lineitem[filter l_commitdate < l_receiptdate]` then `[filter o_orderdate in
Q3'93]` never pushed `o_orderdate` onto `orders` — profiling showed the semi
join running on the full 3,000,000-row `orders` scan (`rows=3000000`) and
producing 2,750,325 matches, filtered down to 105,917 only *after* the join
(`ab_queries.py` measured this at 385ms vs 97ms with the fix).

There was no correctness reason for the exclusion, just no implementation:
a Semi/Anti join's left side is exactly its "preserved" side — a row
survives or doesn't, and the right side never appears in the output, only
in the existence test — so a conjunct reading only left columns commutes
with the join for the same reason it does for a `Left` join (already
handled). Dropping a row before or after the existence test changes nothing
about which surviving rows are kept, since the test never reads the
filtered columns.

**Fix**: allow `Semi`/`Anti` through the join-kind gate, and route them
through the same "only the preserved side may pre-filter" branch already
used for `Left` (a `Right`-destined or `BothSides` conjunct stays above
rather than being pushed into the right/probe side — that push is likely
also sound, equivalent to adding the conjunct as an extra join condition,
but is a separate argument this pass doesn't make yet, so it's left for
later). This is also what improved q21/q22 (both use semi/anti joins for
`EXISTS`/`NOT EXISTS`), unprompted — found via the full-suite `ab_queries.py`
run after the q04 fix, not chased separately.

## What NOT to do

- Don't re-enable or investigate `predicate_pushdown_pass.cpp` — it's dead,
  correctly disabled, and irrelevant. `join_pushdown.cpp` is the real thing.
- Don't credit the cardinality fix (Bug #1) for the q02/q04 regression —
  it's real but doesn't touch this file's actual bottleneck. Verify with
  `ab_queries.py --base <without-fix> --target <with-fix>` on the specific
  file before claiming a fix moved the number.
- Don't trust a single `time` invocation — WSL2 drifts. Use
  `taskset -c 0-7` plus `ab_queries.py`'s paired/interleaved repeats, and
  re-run anything under p≈0.02-0.05 at higher `--repeats` before reporting
  it (q05's initial "+3.8% SLOWER, p=0.023" was noise, gone at 20 repeats).
- Don't reuse `ir::required_columns()`'s demand set as an exact schema
  without intersecting against a proven (`is_known() && !is_open()`) schema
  first — it over-approximates by design for its actual job, and treating
  its output as authoritative broke 4 queries (q15/17/21/22) the first time.
- If you touch `join_pushdown.cpp`'s join-kind gate again, a **right**-side
  push through Semi/Anti has a plausible soundness argument (equivalent to
  an extra join condition) but is unproven here — don't assume it's safe by
  analogy to the left-side fix without working through it, and don't assume
  the currently-untouched Outer/Cross/Asof exclusions are just missing
  implementation the way Semi/Anti's was — Outer specifically needs the
  null-extension argument `Left`/`Right` already make, which doesn't apply
  the same way.
