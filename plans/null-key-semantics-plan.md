# Null-aware keys (group-by, distinct, order, join)

**Status: COMPLETE. All five stages are implemented and covered.** Verified
against the tree on 2026-09-04 by running `tests/data/null_keys_check.ibex`,
which exercises every case below and pits each null against a GENUINE zero:
group-by (3 groups), `update ... by` (null group totals 30), `distinct` (3
rows), `order` asc and desc (nulls last both ways, the position does not flip),
inner join (null keys drop, 3 rows) and left join (null-filled, 5 rows), dcast
(null row key preserved), asof join (null equality key unmatched), and a
multi-column key where `{0,null}` and `{null,0}` stay distinct from each other
and from `{0,0}`. `tests/test_null_keys.cpp` covers the same ground as unit
tests, and `scripts/ibex-e2e.sh` runs the fixture.

This file had carried no status marker at all, which is why the plans index
listed it as "Unverified" long after it was done. Kept for the semantics
section below -- the group-by/join inconsistency is a decision worth being able
to cite -- and it is a removal candidate under the index's own convention that
completed plans leave the tree.

## The bug

Every operator that builds a key from a column reads the column's **value** and
never consults its **validity bitmap**. A null slot holds the type's zero value
(the CSV reader's long-standing convention, which `read_parquet` now matches), so
a null key is indistinguishable from a genuine `0` / `""`.

Reproduced with `read_csv` alone — no Parquet involved — on `k = 0, null, 0, 5, null`:

| operation | today | correct |
| --- | --- | --- |
| `t[select {n=count()}, by {k}]` | 2 groups: `0→4`, `5→1` | 3 groups: `0→2`, `null→2`, `5→1` |
| `t[distinct k]` | 2 rows: `0`, `5` | 3 rows: `0`, `null`, `5` |
| `t[order {k}]` | nulls interleaved with the `0`s | nulls last |
| `t join d on k` | null key matched `d`'s `k=0` row | null key matches nothing |

The nulls do not merely sort oddly — they **silently merge into the zero group**,
and the null group disappears from the result entirely.

This predates the Parquet work: `read_csv` has always attached validity correctly
and these paths have always ignored it.

## Semantics (decided)

- **group-by / distinct** — nulls are equal to each other and distinct from every
  real value: one null group, one null distinct row. (SQL, Polars, pandas all agree.)
- **join** — a null key matches nothing, including another null. Inner join drops
  it; outer join null-fills. This is SQL (`NULL = NULL` is not true) and Polars'
  default (`join_nulls=False`). Deliberately inconsistent with group-by — that
  inconsistency is the SQL standard, not an oversight.
- **order** — nulls sort last, on both `asc` and `desc` (the null position does not
  flip with direction). Polars' and pandas' default.

## Design

Do not teach every fast path about nulls. Instead, at each operator's entry,
detect whether any key column actually has nulls:

- **No null keys (the overwhelmingly common case, and every TPC-H query)** — take
  exactly the paths that run today, untouched. Zero cost, zero risk.
- **Null keys present** — take a null-aware path.

For `aggregate_table` that means: when any group-key column has nulls, bypass the
single-key fast paths (dense-categorical and hashed-categorical) and fall through
to the general factorized multi-key path, which already handles `n_keys == 1`.
Then make **one** thing null-aware — that path's per-column code assignment — by
reserving a dedicated code for null, and emit the key column's validity when
rebuilding the output. One place to get right instead of four.

## Stages

1. `aggregate_table` group keys (aggregate.cpp) — the silent-merge case.
2. `distinct` (interpreter.cpp).
3. `order` (sort.cpp) — nulls last.
4. `join` (join.cpp) — null keys match nothing.
5. `update ... by` (update.cpp) and dcast keys (reshape.cpp) — same grouping rule.

Each stage lands with a regression test built on `read_csv`, because Ibex array
literals cannot express a null.
