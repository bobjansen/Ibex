# Subquery queries — experiment complete

This directory held drafts of the TPC-H queries that use a subquery, written
against the official SF-1 qualification parameters. **Every one has shipped to
`../` and passes the official answer check.** The drafts are gone; this note is
the retrospective.

## The conclusion: almost no new *syntax* was needed

Nine queries were thought to be blocked on subquery syntax. Building them turned
up something better — most were blocked on nothing, and the rest on two small,
general primitives, not on subquery syntax at all:

| Query | What it actually needed |
|---|---|
| Q2, Q17 | **Correlated scalar subquery** (`scalar(...)` + `outer(...)`) — built (SPEC 5.7). |
| Q11 | **Uncorrelated scalar subquery** — the same feature with no capture (a cross join). |
| Q20 | The correlated scalar (composite capture) + two `in`s, each a plain **semi join**. |
| Q22 | **`substring`** (built, SPEC 12.6) + an uncorrelated scalar + a `not exists` as an **anti join**. |
| Q4 | Nothing — an equality `exists` **is** a semi join. |
| Q16, Q18 | Nothing — an `in` / `not in` against a fixed set **is** a semi / anti join. |
| Q21 | Nothing — its inequality-correlated `exists` / `not exists` rewrites to per-order supplier counts. The shipped file follows Polars' line-count shape rather than the exact distinct-supplier form; its header explains the difference. |

So the whole "subquery syntax" project reduced to two engine features — the
correlated scalar subquery and `substring` — plus the realisation that `exists`,
`not exists`, and `in` over a computed set are semi/anti joins that Ibex already
had. The queries read a little further from their SQL than they would with the
sugar, but they run, and correctly.

## What is still only a proposal (and needs no query)

`exists` / `!exists` have since been built as first-class terms (SPEC 5.8);
only the inequality-correlated form (q21's shape) is still unbuilt — see the
exists entry under "Complete" in `plans/README.md`. One design doc survives,
for *ergonomics*, since no remaining query needs it:

- `plans/in-subquery-plan.md` — `in` / `not in` as first-class terms. The one
  genuinely new operator it proposes is a **null-aware anti join** (a plain anti
  join is only exact when the subquery column is non-null — which is why Q16 and
  Q22 could use one). No current query needs it.

## The whole suite is now in

Q7, Q8, Q12, Q14, Q15 — the remaining ordinary join/aggregate queries — have all
been transcribed, so the corpus now holds all 22 TPC-H queries. Q15 turned out to
use an uncorrelated `scalar` after all (its top-supplier test is
`total_revenue = (select max(total_revenue) …)`), which the correlated-subquery
work already covers; the other four are plain joins, `year(...)`, and CASE sums
written as `value * Int64(<predicate>)`.
