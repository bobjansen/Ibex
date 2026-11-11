# Ibex VS Code Extension

Syntax highlighting for `.ibex` files.

## Install locally (no Marketplace needed)

Copy the extension folder into VS Code's extensions directory, then restart VS Code:

```bash
# Linux / WSL
cp -r editors/vscode ~/.vscode/extensions/ibex-language-0.2.0

# macOS
cp -r editors/vscode ~/.vscode/extensions/ibex-language-0.2.0
```

Open any `.ibex` file — it should be highlighted automatically.

Upgrading from 0.1.0: remove `~/.vscode/extensions/ibex-language-0.1.0` first,
or VS Code may keep loading the old grammar.

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
