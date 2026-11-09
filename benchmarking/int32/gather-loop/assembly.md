# Physical-pointer gather loops

The Release git-comparison build enables `-march=native`. Both physical widths
are dispatched before selection iteration. The selected-row mask loop has six
instructions in each specialization; only the load differs. There is no
IntegerView offset load, additional offset address calculation, or width test
in either loop. The full selection visitor is 426 bytes per width.

## Int64 input → Int64 output

```asm
tzcnt  %r10,%r11
mov    (%r9,%r11,8),%r11
mov    %r11,(%rax,%rdi,8)
inc    %rdi
blsr   %r10,%r10
jne    6c1d20
```

## Int32 input → Int64 output

```asm
tzcnt  %r10,%r11
movslq (%r9,%r11,4),%r11
mov    %r11,(%rax,%rdi,8)
inc    %rdi
blsr   %r10,%r10
jne    6c1b80
```

The ordinary AVX2 Release build, which does not enable BMI1, uses `lea`/`and`
in place of `blsr` (seven instructions). This is the same bit-clear difference
noted in the earlier diagnostic. Physical width and offset handling remain
outside the row loops in that build too.
