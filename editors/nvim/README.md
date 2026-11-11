# Ibex for Neovim (and Vim)

Syntax highlighting, filetype detection and comment settings for `.ibex`
files. Plain Vim syntax, so no Tree-sitter grammar is needed.

## Install

Add this directory to the runtime path. With
[lazy.nvim](https://github.com/folke/lazy.nvim), as e.g.
`~/.config/nvim/lua/plugins/ibex.lua`:

```lua
return {
  { dir = vim.fn.expand("~/ibex/editors/nvim"), name = "ibex", lazy = false },
}
```

`lazy = false` loads it at startup so filetype detection is in place before
the first `.ibex` buffer. If `lua/plugins/` held no other spec, this file
also cures lazy's `No specs found for module "plugins"` startup error.

Without a plugin manager, in `init.lua`:

```lua
vim.opt.rtp:append("/path/to/ibex/editors/nvim")
```

Vim 8.2+ (no Lua filetype detection there), in `.vimrc`:

```vim
set rtp+=/path/to/ibex/editors/nvim
autocmd BufRead,BufNewFile *.ibex setfiletype ibex
```

## What it covers

- `//` and `/* */` comments, `"strings"` with escapes
- `` `quoted names` `` and `` `templates ${expr}` `` with the expression highlighted
- integer, float and duration literals (`5m`, `250ms`, `1mo`)
- types, block clauses (`filter`, `select`, `update`, `by`, `window`, ...),
  joins, `let`/`fn`/`extern`/`import`, `case`/`else`
- calls (`mean(`), namespaces (`csv::read`), `RoundMode` and its members
- operators, including `^` (scope escape) and `=>`

`commentstring` is `// %s`, so `gc` comments lines in Neovim 0.10+.

The keyword lists mirror `src/parser/lexer.cpp`; update `syntax/ibex.vim`
when the lexer gains a keyword.
