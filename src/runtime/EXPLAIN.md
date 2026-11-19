# Reading `explain physical`

Companion to [CONTRACTS.md](CONTRACTS.md), which requires every physical-plan
node to declare its fan-out policy and decline reason, and
[PARALLELISM.md](PARALLELISM.md), which explains who owns those decisions
(["Parallelism as a plan decision"](PARALLELISM.md#parallelism-as-a-plan-decision)).
This file describes what the rendering looks like. Rendering code:
`explain_physical` / `explain_breaker` in `physical_plan.cpp`.

## What it prints

`:explain <expr>` in the REPL prints `explain_physical(plan)` under the heading
"Physical plan (capability; runtime fan-out may differ)". It shows the
**unresolved** capability: a `Plan` off `plan_physical` has no
`ExecutionContext`, so caps read `cap=unresolved`. The builder resolves a local
copy at construction, and only that copy knows the real cap.

Each fan-out phase prints one line plus an estimate line:

```
  dedup: parallel-capable  cap=unresolved  floor 32768  partitions=derived  packed-key
         input estimate 4 rows (table)
  ^name  ^permission       ^worker cap     ^row floor   ^always derived     ^PartitionStrategy
```

- **Estimate line:** `input estimate N rows (table|footer|child)`, or
  `no row estimate -> decided on first chunk`. A phase that admits on groups
  reads `no row estimate -> decided on the group count after input`.
- **Resolved forms**, as the builder's copy renders them: `cap<=N` in place of
  `cap=unresolved`, or the whole line replaced by `serial (single core)` /
  `serial (estimate 4 < floor 32768)`, with no estimate line.
- **`floor (shared parallel_min_rows)`** means the phase has no floor of its
  own and inherits the shared knob.

What each plan node shows ([samples](#samples) below):

| Node                                          | Prints                                                                                                                                |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `Breaker(Distinct)`                           | one `dedup` phase                                                                                                                     |
| `Breaker(Order)`                              | one descriptive `sort` phase (not authoritative)                                                                                      |
| `Breaker(Join)`                               | strategy and branch, the typed `HashBuild -> HashProbe` edge, resolved key columns, a `HashBuild` and a `HashProbe` phase             |
| `Breaker(Aggregate)`                          | adaptive strategy, the typed hash-fallback chain and its edges, one phase per node (Discovery, Accumulation, FinalOrdering, Emission) |
| `Breaker(Head)` and the other serial breakers | `serial (single-operator breaker, no fan-out point)`                                                                                  |
| `MapPipeline`                                 | the fused steps, the source, `mode:` with the morsel-parallel step range, the source signature                                        |
| `MaterializedCall(...)`                       | the fallback and why the root is not a map                                                                                            |

Known gap: Distinct and Order do not print their key list. `DistinctNode` and
`OrderNode` do not carry it as an accessible field (it lives in the child
columns / the order keys), and `explain_breaker` does not walk those.

## Samples

Unresolved output, as `:explain` prints it.

**Distinct** over a registered 4-row table:

```
Breaker(Distinct)
  dedup: parallel-capable  cap=unresolved  floor 32768  partitions=derived  packed-key
         input estimate 4 rows (table)
```

**Order** — one descriptive phase, no per-breaker ceiling, floor inherited:

```
Breaker(Order)
  sort: parallel-capable  cap=unresolved  floor (shared parallel_min_rows)  partitions=derived  row-range (sort + gather)
         no row estimate -> decided on first chunk
```

**Head / Tail / TopK / FilterHead / FilterTail:**

```
Breaker(Head)
  serial (single-operator breaker, no fan-out point)
```

**Join** — floors from `JoinThresholdPolicy` (see
[PARALLELISM.md, join and sort](PARALLELISM.md#join-and-sort)), cap `min(budget, pool, 64)`;
`resolved_join_parallelism` fills a `physical::JoinParallelism` at build time.
A streaming left join prints the same with `branch=SingleKeyLeft`.

```
Breaker(Join)
  Join(StreamingProbe branch=SingleKeyInner keys=1 left=Scan right=Scan orientation=runtime)
  edge: HashBuild.RuntimeOrientedBuildOutput -> HashProbe.build_input
  columns: resolved  left[0]=right[0]  output=3
  HashBuild: parallel-capable  cap=unresolved  floor 131072  partitions=derived  head-table (partition by key hash)
         no row estimate -> decided on first chunk
  HashProbe: parallel-capable  cap=unresolved  floor 16384  partitions=derived  range (contiguous probe-row slices)
         no row estimate -> decided on first chunk
```

**Aggregate** — `StreamingSorted` is the adaptive strategy: group-at-a-time
when the chunks arrive sorted on the keys, the hash fallback otherwise.

```
Breaker(Aggregate)
  Aggregate(StreamingSorted)
  hash-fallback: Discovery -> Accumulation -> FinalOrdering -> Emission
    edge: InputChunks -> DiscoveredGroups -> AccumulatedGroups -> OrderedGroups -> OutputChunks
  Discovery: parallel-capable  cap=unresolved  floor 32768  partitions=derived  radix-hash
         input estimate 4 rows (table)
  Accumulation: parallel-capable  cap=unresolved  floor 131072  partitions=derived  morsel (deterministic row ranges)
         input estimate 4 rows (table)
  FinalOrdering: parallel-capable  cap=unresolved  floor 65536  partitions=derived  owned
         input estimate 4 rows (table)
  Emission: parallel-capable  cap=unresolved  floor (shared parallel_min_rows)  partitions=derived  column-range (output column x group range)
         input estimate 4 rows (table)
```

**A map chain and a fallback:**

```
MapPipeline
  Filter+Project(fused)
  source: TableScan(t)
  mode: morsel-parallel(steps 0..1 of 1)
  source signature: fixed-width/all-valid fixed-width/all-valid

MaterializedCall(Melt: root is not a row-local map)
```
