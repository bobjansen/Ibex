# Ibex VS Code Extension

Syntax highlighting for `.ibex` files.

## Install locally (no Marketplace needed)

Copy the extension folder into VS Code's extensions directory, then restart VS Code:

```bash
# Linux / WSL
cp -r editors/vscode ~/.vscode/extensions/ibex-language-0.1.0

# macOS
cp -r editors/vscode ~/.vscode/extensions/ibex-language-0.1.0
```

Open any `.ibex` file — it should be highlighted automatically.

## What is highlighted

| Token | Scope | Typical colour |
|-------|-------|----------------|
| `//` comments | `comment.line` | grey / green italic |
| `"strings"` | `string.quoted.double` | orange / green |
| `` `backtick names` `` | `variable.other.quoted` | cyan |
| `true` / `false` | `constant.language` | blue |
| Duration literals (`1m`, `5s`) | `constant.numeric.duration` | green |
| Numeric literals | `constant.numeric` | green |
| Type names (`Int`, `DataFrame`, …) | `storage.type` | blue / cyan |
| Built-in functions (`mean`, `rolling_sum`, …) | `support.function.builtin` | yellow |
| Clause keywords (`filter`, `select`, `by`, …) | `keyword.control.clause` | purple |
| Join keywords (`join`, `left`, `asof`, `on`) | `keyword.control.join` | purple |
| Declaration keywords (`let`, `extern`, `fn`) | `keyword.other.declaration` | pink |
| Logical operators (`and`, `or`, `not`) | `keyword.operator.logical` | purple |
| Direction keywords (`asc`, `desc`) | `keyword.other.direction` | pink |
| Operators (`->`, `+`, `==`, …) | `keyword.operator` | grey / white |

Exact colours depend on your active theme.
