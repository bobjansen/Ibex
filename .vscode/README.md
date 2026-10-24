# VS Code + Clang

Open the repository folder in VS Code and install its recommended extensions
(Extensions: Show Recommended Extensions). On Linux, install the matching tools:

```sh
sudo apt-get install clang-21 clangd-21 clang-tidy-21 clang-format-21 cmake ninja-build
```

The workspace uses Clang 21 tools on this machine. The clangd extension can use
its downloaded server (already installed on this machine) or one on PATH.
For another LLVM version, adjust the tidy task command and optionally set
`clangd.path`; `clang` / `clang++` come from PATH.
CodeLLDB supplies its own debugger; a separate system LLDB is not required.
The optional CMake formatter extension requires `cmake-format` (for example,
install it with `pipx install cmakelang`). C++ formatting has no Python dependency.

Select **Clang Debug (editor)** with **CMake: Select Configure Preset**.
CMake configures on open and writes `build/compile_commands.json`, which the
existing `.clangd` configuration reads. clangd provides completion, navigation,
rename, inlay hints, include fixes, clang-tidy diagnostics and quick fixes.
C/C++ extension IntelliSense is disabled to avoid duplicate diagnostics.
Formatting on save uses clangd and the repository's `.clang-format`.
The existing `.clang-tidy`, `.clangd` and test-specific tidy rules remain authoritative.

The editor Debug preset disables the optional UI, Python bridge, Parquet and
LightGBM builds to keep configuration and builds light. Override these options
in a local, ignored `CMakeUserPresets.json` when working on those components.
The Release preset keeps project defaults and uses `build-release/`; use Release
for benchmarks. The sanitizer preset uses a separate `build-sanitizers/` directory.
clangd intentionally always reads Debug flags from `build/`, even when another
preset is selected in CMake Tools.

- **Ctrl+Shift+B**: configure and build Debug.
- **Tasks: Run Test Task**: build tests and run CTest excluding the `slow` label.
- **Tasks: Run Task**: full tests, Release, ASan/UBSan, standalone tidy, and format checks.
- **F5**: debug the REPL or tests; the test launcher prompts for a Catch2 filter.
- **Testing** sidebar: CMake Tools discovers CTest tests after configuration/build.

Run full tests before finishing parser, lexer, AST, IR or codegen changes.
The standalone tidy task expects a C/C++ translation unit; use clangd for header
diagnostics. The format wrapper selects an installed clang-format.

Equivalent terminal workflow:

```sh
cmake --preset clang-debug
cmake --build --preset clang-tests
ctest --preset clang-fast
```

For `.ibex` syntax highlighting, install the local extension as described in
[`editors/vscode/README.md`](../editors/vscode/README.md).

References: [clangd setup](https://clangd.llvm.org/installation),
[CodeLLDB manual](https://github.com/vadimcn/codelldb/blob/master/MANUAL.md).
