" SPDX-License-Identifier: AGPL-3.0-only
" Copyright (C) 2026 Bob Jansen

if exists('b:did_ftplugin')
  finish
endif
let b:did_ftplugin = 1

setlocal commentstring=//\ %s
setlocal comments=s1:/*,mb:*,ex:*/,://
setlocal formatoptions-=t formatoptions+=croql

let b:undo_ftplugin = 'setlocal commentstring< comments< formatoptions<'
