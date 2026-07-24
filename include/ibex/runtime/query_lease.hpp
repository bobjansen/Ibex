#pragma once

#include <atomic>

namespace ibex::runtime {

/// One query at a time: a process-wide guard on top-level query execution.
///
/// The runtime executes at most one in-flight `interpret()` (later, executor)
/// invocation per process — see the runtime multithreading plan, Phase 0 item 6.
/// A second *top-level* entry is rejected with a stable error rather than
/// serialized. Two things trigger that rejection:
///
///   * once workers exist, a second thread entering `interpret()` while a query
///     is already running; and
///   * a re-entrant entry from an extern/plugin that calls back into
///     `interpret()` from inside the query it is running under — it could not
///     wait for the pool lease it already holds without deadlocking.
///
/// Because there is never more than one live query, the process-wide interrupt
/// flag (`interrupt.hpp`) needs no per-query scoping: it means "cancel the one
/// running query." This lease is the enforcement of that single-live-query
/// invariant, which the LazyTable cache freeze/thaw and the deferred-scan
/// ownership both rely on.

namespace detail {
// One instance process-wide (inline variable). False when no query is running.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline std::atomic<bool> query_in_flight{false};
}  // namespace detail

/// RAII claim on the single query slot. Construction attempts the claim in one
/// atomic step; `held()` reports whether it succeeded. A caller that did not
/// get the lease (`!held()`) must reject and must not execute — it holds
/// nothing, so its destructor releases nothing. A held lease releases on
/// destruction, so the slot is freed even if the query unwinds via an error or
/// exception.
class QueryExecutionLease {
   public:
    QueryExecutionLease() noexcept {
        bool expected = false;
        // acq_rel on success publishes/pairs with the releasing store below so a
        // subsequent query observes the previous one's writes; acquire on
        // failure is enough to see that the slot is taken.
        held_ = detail::query_in_flight.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
    }
    QueryExecutionLease(const QueryExecutionLease&) = delete;
    QueryExecutionLease(QueryExecutionLease&&) = delete;
    auto operator=(const QueryExecutionLease&) -> QueryExecutionLease& = delete;
    auto operator=(QueryExecutionLease&&) -> QueryExecutionLease& = delete;
    ~QueryExecutionLease() {
        if (held_) {
            detail::query_in_flight.store(false, std::memory_order_release);
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
