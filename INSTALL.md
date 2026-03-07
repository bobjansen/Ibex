# Install

This project uses a standard CMake + Ninja build. The core Ibex build is
independent of plugins. Plugins can be built separately.

## Requirements

- Clang 20
- CMake 3.31+
- Ninja

## Build (Debug)

```
cmake -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

## Build (Release)

```
cmake -B build-release -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
```

## REPL

```
./build/tools/ibex
```

## CSV and JSON Plugins (built with core)

Both CSV and JSON plugins are built automatically with the core:

```
IBEX_LIBRARY_PATH=./build/tools ./build/tools/ibex
```

```ibex
import "csv";
import "json";
let df = read_csv("data.csv");
write_json(df, "data.json");
```

## Parquet Plugin (standalone)

```
./scripts/ibex-parquet-build.sh
IBEX_LIBRARY_PATH=./libraries ./build/tools/ibex
```

## End-to-End Checks

```
./scripts/ibex-e2e.sh
```
