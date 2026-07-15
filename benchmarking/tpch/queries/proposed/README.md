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
| Q21 | Nothing — its inequality-correlated `exists` / `not exists` rewrites to per-order **distinct-supplier counts** (the same distinct+count as Q16). |

So the whole "subquery syntax" project reduced to two engine features — the
correlated scalar subquery and `substring` — plus the realisation that `exists`,
`not exists`, and `in` over a computed set are semi/anti joins that Ibex already
had. The queries read a little further from their SQL than they would with the
sugar, but they run, and correctly.

## What is still only a proposal (and needs no query)

Two design docs survive, both for *ergonomics*, since no remaining query needs
them:

- `plans/exists-subquery-plan.md` — `exists` / `not exists` as first-class terms.
  Zero new queries: the equality cases are semi/anti joins, and Q21's inequality
  case has the distinct-count rewrite above. A *generic* inequality-correlated
  `exists` (Tier 3) would still want a `row_number()` Ibex lacks, but no TPC-H
  query forces it.
- `plans/in-subquery-plan.md` — `in` / `not in` as first-class terms. The one
  genuinely new operator it proposes is a **null-aware anti join** (a plain anti
  join is only exact when the subquery column is non-null — which is why Q16 and
  Q22 could use one). No current query needs it.

## Not yet written (and not subquery-related)

Q7, Q8, Q12, Q14, Q15 are absent from the corpus, but none of them uses a
subquery — they are ordinary join/aggregate queries that simply have not been
transcribed yet.
