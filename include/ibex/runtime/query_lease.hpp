#pragma once

namespace ibex::runtime {

/// One query at a time: a host-runtime guard on top-level query execution.
///
/// The host runtime executes at most one in-flight `interpret()` (later,
/// executor) invocation — see the runtime multithreading plan, Phase 0 item 6.
/// A second *top-level* entry is rejected with a stable error rather than
/// serialized. Two things trigger that rejection:
///
///   * once workers exist, a second thread entering `interpret()` while a query
///     is already running; and
///   * a nested entry attempted while the host query owns the lease. Calling
///     `interpret()` from an extern/plugin callback is unsupported; plugins
///     provide data/functions to their host query and do not start queries.
///
/// Because there is never more than one live host query, the process-wide interrupt
/// flag (`interrupt.hpp`) needs no per-query scoping: it means "cancel the one
/// running query." This lease is the enforcement of that single-live-query
/// invariant, which the LazyTable cache freeze/thaw and the deferred-scan
/// ownership both rely on.

namespace detail {
// Backed by the host runtime implementation, rather than an inline variable in
// this public header. Bundled plugins link static runtime code too; they are
// forbidden from initiating interpretation, so query ownership belongs to the
// embedding runtime that owns the top-level entry point.
[[nodiscard]] auto try_claim_query_execution() noexcept -> bool;
auto release_query_execution() noexcept -> void;
}  // namespace detail

/// RAII claim on the single query slot. Construction attempts the claim in one
/// atomic step; `held()` reports whether it succeeded. A caller that did not
/// get the lease (`!held()`) must reject and must not execute — it holds
/// nothing, so its destructor releases nothing. A held lease releases on
/// destruction, so the slot is freed even if the query unwinds via an error or
/// exception.
class QueryExecutionLease {
   public:
    QueryExecutionLease() noexcept { held_ = detail::try_claim_query_execution(); }
    QueryExecutionLease(const QueryExecutionLease&) = delete;
    QueryExecutionLease(QueryExecutionLease&&) = delete;
    auto operator=(const QueryExecutionLease&) -> QueryExecutionLease& = delete;
    auto operator=(QueryExecutionLease&&) -> QueryExecutionLease& = delete;
    ~QueryExecutionLease() {
        if (held_) {
            detail::release_query_execution();
        }
    }

    /// True when this lease owns the single query slot. Only an owner may run.
    [[nodiscard]] auto held() const noexcept -> bool { return held_; }

   private:
    bool held_ = false;
};

/// Error string returned when a query is rejected because another is already
/// running. Kept exact so callers (and tests) can distinguish it from ordinary
/// failures.
[[nodiscard]] inline auto query_in_flight_message() -> const char* {
    return "runtime is already executing a query";
}

}  // namespace ibex::runtime
