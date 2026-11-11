" SPDX-License-Identifier: AGPL-3.0-only
" Copyright (C) 2026 Bob Jansen
"
" Vim/Neovim syntax for Ibex (.ibex). Keywords mirror the lexer's table in
" src/parser/lexer.cpp; keep the two in sync.

if exists('b:current_syntax')
  finish
endif

let s:cpo_save = &cpo
set cpo&vim

syn case match

" Literals. Durations come after numbers so `5m` wins over `5`.
syn keyword ibexBoolean     true false
syn keyword ibexNull        null NULL
syn match   ibexNumber      "\<\d\+\>"
syn match   ibexFloat       "\<\d\+\.\d\+\%([eE][+-]\=\d\+\)\=\>"
syn match   ibexFloat       "\<\d\+[eE][+-]\=\d\+\>"
syn match   ibexDuration    "\<\d\+\%(ns\|us\|ms\|mo\|[smhdwy]\)\>"

" Types.
syn keyword ibexType        Int Int32 Int64 Float32 Float64 Bool String Date
syn keyword ibexType        Timestamp Decimal Series Column DataFrame TimeFrame Stream

" Declarations and modules.
syn keyword ibexDeclaration let mut fn extern
syn keyword ibexInclude     import from

" Block clauses: t[filter ..., select { ... }, by ...].
syn keyword ibexClause      filter select update distinct order head tail by
syn keyword ibexClause      window resample rename melt dcast cov corr transpose
syn keyword ibexClause      model map in where

" Joins.
syn keyword ibexJoin        join left right outer semi anti cross asof on suffix

" Other keywords.
syn keyword ibexConditional case else
syn keyword ibexKeyword     as is not asc desc

" Calls, namespaces (csv::read) and built-in enums (RoundMode::Bankers).
syn match   ibexFunction    "\<\h\w*\ze\s*("
syn match   ibexNamespace   "\<\h\w*\ze::"
syn match   ibexEnum        "\<RoundMode\>"
syn keyword ibexEnumMember  Nearest Bankers Floor Ceil Trunc

" Operators. `^` is the scope-escape operator; `=>` the case arm.
syn match   ibexDelimiter   "[;,:.]"
syn match   ibexOperator    "[-+*/%<>=!^~]"
syn match   ibexOperator    "->\|=>\|::\|==\|!=\|<=\|>=\|&&\|||"

" Later definitions win at the same start column, so operators come before
" strings and comments ("/" vs "//") and plain backticks before templates.
" Double-quoted strings are plain literals with escapes.
syn match   ibexEscape      contained +\\["\\nrt0]+
syn region  ibexString      start=+"+ skip=+\\\\\|\\"+ end=+"+ contains=ibexEscape,@Spell oneline

" Backticks: a quoted identifier, or a template when it holds ${expr}.
syn region  ibexInterp      contained matchgroup=ibexInterpDelim start="\${" end="}" contains=TOP
syn match   ibexQuotedIdent "`[^`\n]*`"
syn region  ibexTemplate    start="`\ze[^`]*\${" end="`" contains=ibexInterp oneline

" Comments: // to end of line and /* ... */ (not nested).
syn keyword ibexTodo        contained TODO FIXME XXX NOTE
syn match   ibexLineComment "//.*$" contains=ibexTodo,@Spell
syn region  ibexBlockComment start="/\*" end="\*/" contains=ibexTodo,@Spell

syn sync minlines=200

hi def link ibexTodo         Todo
hi def link ibexLineComment  Comment
hi def link ibexBlockComment Comment
hi def link ibexString       String
hi def link ibexEscape       SpecialChar
hi def link ibexTemplate     String
hi def link ibexInterpDelim  Special
hi def link ibexQuotedIdent  Identifier
hi def link ibexBoolean      Boolean
hi def link ibexNull         Constant
hi def link ibexNumber       Number
hi def link ibexFloat        Float
hi def link ibexDuration     Number
hi def link ibexType         Type
hi def link ibexDeclaration  Keyword
hi def link ibexInclude      Include
hi def link ibexClause       Statement
hi def link ibexJoin         Statement
hi def link ibexConditional  Conditional
hi def link ibexKeyword      Keyword
hi def link ibexFunction     Function
hi def link ibexNamespace    Identifier
hi def link ibexEnum         Type
hi def link ibexEnumMember   Constant
hi def link ibexOperator     Operator
hi def link ibexDelimiter    Delimiter

let b:current_syntax = 'ibex'

let &cpo = s:cpo_save
unlet s:cpo_save
