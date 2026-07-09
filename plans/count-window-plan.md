# Per-call rolling windows: count and duration

## Goal

Let rolling aggregates take an explicit per-call window as a trailing argument,
in two flavours, discriminated by the literal kind:

```
rolling_mean(px)          # window comes from the enclosing `window 5s` block (today)
rolling_mean(px, 20)      # last-20-rows COUNT window   — no TimeFrame required
rolling_mean(px, 60s)     # 60-second DURATION window   — TimeFrame required
```

This unlocks the N-observation TA vocabulary (14-period RSI, 20-period Bollinger,
…), lets rolling functions be used in a plain `update` with no `window` block or
`as_timeframe()`, and lets different fields use different windows in one `update`
(which the block form cannot express). Streaming inherits it for free — the
`Stream` event loop reuses the batch interpreter.

## Why this is small

Every one of the 11 rolling aggregates already consumes a two-pointer `[lo, i]`
window; the aggregate maths is independent of *how* `lo` is chosen. A count
window is just a different `lo` rule. Two mechanisms exist in `window.cpp`:

- monotonic two-pointer (count/mean/sum/median/std/ewma): inline
  `threshold = time_vals[i] - dur_val; while (lo < i && time_vals[lo] < threshold)`
- `window_lo(time_col, i, duration)` binary search per row (quantile/skew/
  kurtosis/min/max)

Both collapse to a single spec-aware predicate/closure.

## Representation

The IR `ir::Literal` variant has no duration alternative, so a `60s` arg cannot
survive lowering as a positional literal. Instead, **lowering peels the trailing
window arg off a rolling call and re-attaches it as a sentinel named arg**
(same idiom as `rep`'s `__array_len`):

- int literal      → `__window_n`  = <count>            (int64)
- duration literal → `__window_ns` = <nanoseconds>      (int64, via `parse_duration`)

Consequences:
- Positional args are unchanged (`col`, plus `alpha`/`p` for ewma/quantile), so
  `window.cpp`'s existing arg parsing and the type-inference sites
  (`schema.cpp`, `expr.cpp`) need **no** changes.
- Duration is stored pre-parsed in ns — no runtime parsing, no IR variant change.
- Existing `window 5s` + `rolling_mean(px)` produces no named arg → codegen and
  every other consumer are untouched.

### AST base arity (which trailing arg is the window)

The window is the trailing arg only when `args.size() > base` and that arg is an
int/duration literal:

| callee | base positional args |
|---|---|
| `rolling_ewma`, `rolling_quantile` | 2 (`col`, `alpha`/`p`) |
| `rolling_count` | 0 (takes no column) |
| all others | 1 (`col`) |

`rolling_ewma(px, 0.3)` → last arg is a *double*, not int/duration → stays alpha.
`rolling_count(px)` → last arg is an identifier → not a window (back-compat).

## WindowSpec (runtime)

```cpp
struct CountWindow { std::int64_t n; };
using WindowSpec = std::variant<ir::Duration, CountWindow>;
```

`apply_rolling_func` takes a resolved `WindowSpec`. Callers resolve it via:

```cpp
rolling_window_spec(call, block_default)  // reads __window_n/__window_ns,
                                          // else block_default, else error
```

Inside `apply_rolling_func`:
- count mode skips all time-index setup (no TimeFrame needed);
- duration mode keeps the TimeFrame requirement (clear error if absent);
- monotonic sites: `while (lo < i && should_drop(lo, i))` where
  `should_drop = is_count ? (i - lo >= n) : (time_vals[lo] < time_vals[i] - dur)`;
- `window_lo` sites: `win_lo(i) = is_count ? max(0, i+1-n) : window_lo(...)`.

Partial windows at the stream/array start match the existing time-window
behaviour (expanding until the window fills).

## Touch points

1. `interpreter_internal.hpp` — `CountWindow`/`WindowSpec`, new
   `apply_rolling_func(call, table, WindowSpec)` + `rolling_window_spec(...)` decls.
2. `window.cpp` — spec-aware `should_drop`/`win_lo`; guard time-index setup;
   implement `rolling_window_spec`.
3. `update.cpp` — resolve spec in `windowed_update_table` (block default) and in
   the plain-`update` path (no block; count works, duration errors cleanly).
4. `lower.cpp` — `lower_rolling_call`: peel trailing window arg → sentinel named arg.
5. tests — count + per-call duration windows, parity with block form, no-TimeFrame
   count in a plain update.

No new IR node, no parser change, no ABI/layout change (no plugin rebuild),
no type-inference change.

## Follow-ups (out of scope)

- `window N rows { ... }` block syntax (lexer keyword) — deferred; per-call
  subsumes the common case.
- Monotonic-deque `rolling_min`/`rolling_max` (still O(n·w)); count windows
  inherit the same complexity.

## Codegen parity (done)

Per-call windows needed no emitter changes: `CallExpr` named args
(`__window_n`/`__window_ns`) are already passed through generically via
`ops::fn_call`, and `ops::update`/`ops::windowed_update` delegate straight to
`interpret()`, so the compiled path runs the exact same window.cpp code as the
REPL. The one real gap was `window ..., update {...}, by ...`: the emitter
explicitly rejected any windowed update with a `by`-clause. Fixed by threading
`group_by` through `ops::windowed_update` (mirroring plain `ops::update`).
Windowed `update` with tuple-field sources (`(a, b) = Table {...}`) is still
rejected — the interpreter's `WindowNode` handling doesn't support that
combination either, so this is parity, not a gap.
