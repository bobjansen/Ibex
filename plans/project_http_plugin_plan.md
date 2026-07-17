---
name: http_plugin_plan
description: "HTTP plugin MVP plan — simple registration API, blocking server, no decorators yet"
metadata:
  node_type: memory
  type: project
  originSessionId: f49c14ab-9cc0-494a-898a-19adcff31b62
---

# HTTP Plugin Implementation Plan

## Overview

Simple, registration-based HTTP plugin for Ibex that allows users to expose query functions as HTTP endpoints. No decorators (those come later as orthogonal language feature). Registration is explicit and non-blocking; server startup is blocking and user-controlled.

---

## User Experience

### REPL Workflow

```ibex
// Define query functions
fn get_price(symbol: String, days: Int = 30) -> DataFrame {
    prices[filter(ticker == symbol and date >= today() - days)]
}

fn fetch_data(id: Int) -> DataFrame {
    data[filter(id == id)]
}

// Register endpoints (non-blocking, just adds to registry)
http_register_endpoint("/prices/{symbol}", "get_price", method="get", format="json")
http_register_endpoint("/data/{id}", "fetch_data", method="get", format="json")

// Start server (blocking)
http_listen(8080)
// Server now running, handling requests at localhost:8080/prices/... and /data/...
// User can curl or browse
// User presses Ctrl+C to stop

// Back in REPL, can adjust and re-register
fn get_price_v2(symbol: String) -> DataFrame { ... }
http_register_endpoint("/prices/{symbol}", "get_price_v2")
http_listen(8080)  // Start again with updated endpoint
```

### Codegen Workflow (`.ibex` file)

```ibex
import "http"

fn get_price(symbol: String) -> DataFrame {
    prices[filter(ticker == symbol)]
}

fn main() {
    http_register_endpoint("/prices/{symbol}", "get_price", format="json")
    http_listen(8080)  // Blocks; script runs as a server
    // When killed externally, process exits
}
```

---

## Core Design Decisions

### 1. Registration Model (MVP): Quoted String Function Names

**Decision: Use `http_register_endpoint(path, "function_name", ...)`**

- Function names passed as quoted strings
- Registration performs lookup in interpreter's symbol table
- No new language features required (first-class functions not needed yet)
- Path to upgrade: when decorators or function references are added, both styles can work

**Why this approach**:
- Keeps MVP scope focused (no parser/type system changes)
- Matches how Python plugins often work (dynamic lookup)
- Underlying registration machinery will be reused by decorators later
- Clear error messages if function doesn't exist

### 2. Server Lifecycle: Explicit, Blocking Startup

**Decision: Non-blocking registration, blocking server startup**

- `http_register_endpoint()` adds to registry and returns immediately
- `http_listen(port)` starts server and blocks until shutdown
- User controls flow: Ctrl+C to stop, then adjust and re-start

**Why this approach**:
- Intuitive REPL workflow (register → test → break → adjust → repeat)
- No background thread management complexity
- Matches typical CLI server development pattern
- Clear cause-and-effect for debugging

### 3. Parameter Binding: Multi-Source with Precedence

**Decision: Path params → query string → POST body → defaults**

1. **Path parameters** (`{symbol}` in `/prices/{symbol}`) — highest priority, required if in template
2. **Query string** (`?days=30`) — optional, matched by name
3. **POST body** (JSON object) — optional, matched by name
4. **Defaults** — use function's default parameter values

**Type conversion**:
- `String` ← path param, query string, JSON
- `Int`, `Double` ← parse from query string / JSON
- `Bool` ← "true"/"false" / JSON
- `Date` ← "YYYY-MM-DD" / JSON
- `Timestamp` ← ISO8601 / JSON
- Type conversion failures → 400 Bad Request

### 4. Response Format: JSON Primary + Alternatives

**Decision: JSON wrapper for all responses, alternate formats in body**

Success response:
```json
{
  "success": true,
  "data": [
    {"symbol": "AAPL", "price": 150.25},
    {"symbol": "GOOG", "price": 2800.50}
  ],
  "meta": {
    "rows": 2,
    "columns": 2,
    "execution_time_ms": 45
  },
  "error": null
}
```

Error response:
```json
{
  "success": false,
  "data": null,
  "error": "parameter validation failed",
  "details": [
    {"param": "days", "reason": "invalid integer"}
  ]
}
```

Alternate formats (Arrow IPC, CSV, Parquet) in V1.0.

---

## Core API

### Runtime (REPL + `.ibex` scripts)

```ibex
// Register endpoint with function name (quoted string)
extern fn http_register_endpoint(
    path: String,              // "/prices/{symbol}"
    function_name: String,     // "get_price"
    method: String = "get",    // "get", "post", "put", "delete"
    format: String = "json"    // "json", "arrow", "csv"
) -> Int from "http.hpp";

// Start server (blocking)
extern fn http_listen(port: Int) -> Int from "http.hpp";

// Debug endpoint list (optional)
extern fn http_endpoints(port: Int = 0) -> DataFrame from "http.hpp";
```

---

## Implementation Phases

### Phase 1: Core Server & Parameter Binding

**New files**:
- `libs/http/CMakeLists.txt`
- `libs/http/http.hpp` — HTTP server, routing, parameter binding
- `libs/http/http.cpp` — Plugin init, extern functions
- `libs/http/http.ibex` — Public API stub
- `libs/http/tests/test_http_*.cpp` — Unit tests

**Modified files**:
- `CMakeLists.txt` — Add `add_subdirectory(libs/http)` with optional flag

**Deliverables**:
- HTTP server listens on port
- Endpoint registration
- Path parameter extraction
- Query string/POST body parameter parsing
- Type conversion with validation
- Error responses (400, 500)
- Success responses with metadata
- 20+ unit tests

### Phase 2: Result Serialization

**New/Modified files**:
- `include/ibex/plugin/schema.hpp` — Add `table_to_json_array()`
- `libs/http/http.hpp` — Serialization dispatch

**Deliverables**:
- JSON array serialization (primary)
- Response wrapper generation
- Chunked HTTP responses for large results

### Phase 3: Function Lookup & Execution

**Key challenge**: Runtime lookup of function by name

**Solution**:
- `http_register_endpoint()` calls `interpreter()->lookup_function(name)`
- Validates: function exists, returns DataFrame
- Stores function reference for later invocation
- At request time: bind params → call function → serialize result

**Modified files**:
- `libs/http/http.cpp` — Implement lookup and execution
- `src/runtime/` — May need to expose interpreter context if not already available

**Deliverables**:
- Function lookup with error handling
- Works in REPL (dynamic lookup)
- Works in codegen (compiled calls, no lookup needed)

### Phase 4: Integration & Polish

**Deliverables**:
- Cross-plugin queries (HTTP → Kafka, CSV, etc.)
- Metrics (request count, latency)
- REPL examples
- `.ibex` script examples
- Documentation with curl examples

---

## File Structure

```
/home/brj/ibex/
├── libs/http/
│   ├── CMakeLists.txt
│   ├── http.hpp                    # Core server + routing + params
│   ├── http.cpp                    # Plugin init + extern fns
│   ├── http.ibex                   # Public API
│   └── tests/
│       ├── test_http_registration.cpp
│       ├── test_http_params.cpp
│       ├── test_http_errors.cpp
│       └── test_http_formats.cpp
├── include/ibex/plugin/
│   └── schema.hpp                  # (modify) add table_to_json_array()
└── CMakeLists.txt                  # (modify) add http subdirectory
```

---

## Success Criteria (MVP)

- [ ] HTTP server listens on specified port
- [ ] Register endpoints with `http_register_endpoint(path, function_name)`
- [ ] Start server with `http_listen(port)` (blocking)
- [ ] Parameter binding: path params, query string, POST body
- [ ] Type validation: convert strings → Int/Double/Date/etc., 400 errors on failure
- [ ] JSON response format (primary)
- [ ] Success/error response wrapper
- [ ] Works in REPL (register → start → Ctrl+C → adjust → re-register)
- [ ] Works in `.ibex` scripts
- [ ] 20+ unit tests
- [ ] Example REPL session
- [ ] Example `.ibex` script

---

## V1.0 Goals (Future)

- Arrow IPC format
- CSV format
- Parquet format
- POST/PUT/DELETE methods
- Caching with TTL
- Metrics endpoint
- Response compression
- CORS headers

---

## Open Questions

1. **Interpreter Context**: Is there an `Interpreter` singleton or thread-local available from extern functions? If not, need to add one.

2. **Codegen Handler Calls**: How do generated HTTP handlers invoke compiled query functions? (calling convention, linking)

3. **Port Management**: `http_listen()` accepts port parameter?

4. **Thread Pool**: Size = `std::thread::hardware_concurrency()`?

---

## Dependencies

- `cpp-httplib` (MIT) — HTTP server
- `nlohmann/json` (existing)
- `Arrow C++` (existing, for IPC in V1.0)

---

## Notes

- Registration machinery built now will be reused when decorators are added later
- Function name quoting allows upgrade path: string lookup → bare references → decorators
- Simple blocking server model avoids complex lifecycle management
- Each phase is independent and deliverable
