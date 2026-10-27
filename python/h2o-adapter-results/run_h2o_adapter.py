"""Run the h2o questions through Polars streaming and IbexEngine (same resident frames)."""
import argparse, json, math, sys, time, traceback, pathlib, os
sys.path.insert(0, "/home/brj/ibex/benchmarking")
import polars as pl
import ibex_pyarrow
from ibex_polars import IbexEngine
from h2o import run as h2o

ap = argparse.ArgumentParser()
ap.add_argument("data"); ap.add_argument("--questions", nargs="*", default=[])
ap.add_argument("--iters", type=int, default=5); ap.add_argument("--harness-sum", action="store_true", help="keep the harness when/then sum wrapper"); ap.add_argument("--out")
a = ap.parse_args()
task, nas = h2o.parse_data_name(a.data)
if task == "groupby":
    df = (pl.read_csv(h2o.DATA_DIR / f"{a.data}.csv", null_values=h2o.NULL_TOKEN,
          schema_overrides={"id4": pl.Int32, "id5": pl.Int32, "id6": pl.Int32,
                            "v1": pl.Int32, "v2": pl.Int32, "v3": pl.Float64})
          .with_columns(pl.col(["id1", "id2", "id3"]).cast(pl.Categorical)))
    frames = (df.lazy(),)
else:
    t = h2o.join_tables(a.data)
    ints = lambda *c: {k: pl.Int32 for k in c}
    cat = lambda *c: pl.col(list(c)).cast(pl.Categorical)
    rd = lambda p, o: pl.read_csv(p, null_values=h2o.NULL_TOKEN, schema_overrides=o)
    x = rd(t["x"], {**ints("id1","id2","id3"), "v1": pl.Float32}).with_columns(cat("id4","id5","id6"))
    s = rd(t["small"], {**ints("id1"), "v2": pl.Float32}).with_columns(cat("id4"))
    m = rd(t["medium"], {**ints("id1","id2"), "v2": pl.Float32}).with_columns(cat("id4","id5"))
    b = rd(t["big"], {**ints("id1","id2","id3"), "v2": pl.Float32}).with_columns(cat("id4","id5","id6"))
    frames = (x.lazy(), s.lazy(), m.lazy(), b.lazy())

def canon(d):
    return d.select(sorted(d.columns)).sort(sorted(d.columns), nulls_last=True)

def same(x, y):
    from polars.testing import assert_frame_equal
    assert_frame_equal(canon(x), canon(y), check_dtypes=False, rel_tol=1e-6, abs_tol=1e-6)

def timed(fn, n):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter(); r = fn(); ts.append((time.perf_counter()-t0)*1000); del r
    return ts

report = {"data": a.data, "threads": os.environ.get("IBEX_CORES"), "queries": {}}
UPSTREAM = {
 "q1": lambda x: x.group_by("id1").agg(pl.sum("v1")),
 "q2": lambda x: x.group_by(["id1","id2"]).agg(pl.sum("v1")),
 "q3": lambda x: x.group_by("id3").agg(pl.sum("v1"), pl.mean("v3")),
 "q5": lambda x: x.group_by("id6").agg(pl.sum("v1"), pl.sum("v2"), pl.sum("v3")),
 "q10": lambda x: x.group_by(["id1","id2","id3","id4","id5","id6"]).agg(pl.sum("v3"), pl.len().alias("count")),
}
for qid, desc, _, _, pq, _ in h2o.questions_for(task, nas):
    if task == "groupby" and not a.harness_sum and qid in UPSTREAM: pq = UPSTREAM[qid]
    if a.questions and qid not in a.questions: continue
    e = report["queries"][qid] = {"desc": desc}
    try:
        q = pq(*frames)
        eng = IbexEngine()
        t0 = time.perf_counter(); eng.prepare(q); e['prepare_ms'] = (time.perf_counter()-t0)*1000
        e["native"] = eng.explain(q)
        ref = q.collect(engine="streaming")
        got = q.collect(engine=eng)
        try: same(ref, got); e["check"] = "ok"
        except AssertionError as ex: e["check"] = "MISMATCH " + str(ex)[:300]
        del ref, got
        # interleaved: warmup both, then alternate
        for fn in (lambda: q.collect(engine="streaming"), lambda: q.collect(engine=eng)): fn()
        ps, ix = [], []
        for _ in range(a.iters):
            ps += timed(lambda: q.collect(engine="streaming"), 1)
            ix += timed(lambda: q.collect(engine=eng), 1)
        e.update(polars_ms=min(ps), ibex_ms=min(ix), polars_all=ps, ibex_all=ix, ratio=min(ix)/min(ps))
        print(f"{qid:4s} polars {min(ps):9.1f} ms  ibex-polars {min(ix):9.1f} ms  ratio {min(ix)/min(ps):5.2f} prepare {e['prepare_ms']:7.0f} ms {e['check'][:20]}  {desc}", flush=True)
    except Exception as ex:
        e["error"] = f"{type(ex).__name__}: {str(ex)[:400]}"
        print(f"{qid:4s} FAILED {e['error'][:200]}", flush=True)
if a.out: pathlib.Path(a.out).write_text(json.dumps(report, indent=2))
