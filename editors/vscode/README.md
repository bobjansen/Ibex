# Ibex VS Code Extension

Syntax highlighting for `.ibex` files.

## Install locally (no Marketplace needed)

In VS Code, open the Command Palette and run **Developer: Install Extension
from Location...**, then pick this directory (`editors/vscode`). Open any
`.ibex` file; it is highlighted right away.

After pulling grammar changes, reload the window (**Developer: Reload
Window**); if the old highlighting persists, run the install command again.

Copying the folder into `~/.vscode/extensions/` no longer works: VS Code only
loads extensions registered in `~/.vscode/extensions/extensions.json`, so a
copied folder is silently ignored. Remove any such copy
(`ibex-language-0.1.0` from older instructions).

## What is highlighted

| Token | Scope |
|-------|-------|
| `//` and `/* */` comments | `comment.line.double-slash`, `comment.block` |
| `"strings"` with escapes | `string.quoted.double`, `constant.character.escape` |
| `` `templates ${expr}` `` | `string.interpolated`, expression inside `meta.embedded.expression` |
| `` `quoted names` `` | `variable.other.quoted` |
| `true` / `false`, `null` | `constant.language.boolean`, `constant.language.null` |
| Duration literals (`5m`, `250ms`, `1mo`) | `constant.numeric.duration` |
| Integer and float literals (`2`, `1.5`, `1e10`) | `constant.numeric` |
| Types (`Int64`, `Decimal`, `DataFrame`, …) | `storage.type` |
| `RoundMode` and its members | `support.type.enum`, `support.constant.enum` |
| Clauses (`filter`, `select`, `update`, `by`, `window`, `melt`, …) | `keyword.control.clause` |
| Joins (`join`, `left`, `semi`, `asof`, `on`, `suffix`, …) | `keyword.control.join` |
| `let`, `mut`, `fn`, `extern` / `import`, `from` | `keyword.other.declaration` / `keyword.control.import` |
| `case`, `else` | `keyword.control.conditional` |
| `asc`, `desc` / `as`, `is`, `not` | `keyword.other.direction` / `keyword.other` |
| `fn name` | `entity.name.function` |
| Calls (`mean(`, `csv::read(`) | `entity.name.function.call` |
| Namespaces (`csv::`) | `entity.name.namespace` |
| Operators (`->`, `=>`, `&&`, `==`, `^`, `+`, …) | `keyword.operator.*` |

Exact colours depend on your active theme. The keyword lists mirror
`src/parser/lexer.cpp` (and `editors/nvim/syntax/ibex.vim`); update all three
together.
