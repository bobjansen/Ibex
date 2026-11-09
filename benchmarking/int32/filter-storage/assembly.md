# Filter payload copy loops

Native Release comparison build (`-march=native`). Dispatch is outside these
selected-row loops. Compact results use 32-bit loads and stores; no payload
widening or per-row width branch remains.

## int, int

```asm
tzcnt  %r10,%rbx
mov    (%r11,%rbx,4),%ebx
mov    %ebx,(%rax,%rdi,4)
inc    %rdi
blsr   %r10,%r10
jne    6c1ef0
```

## long, long

```asm
tzcnt  %r10,%r11
mov    (%r9,%r11,8),%r11
mov    %r11,(%rax,%rdi,8)
inc    %rdi
blsr   %r10,%r10
jne    6c23b0
```

