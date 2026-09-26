// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <memory>

namespace ibex::runtime {

/// A consumer's hook into a streamed scan pipeline: work the consumer would do
/// on each chunk, done instead on the pipeline worker that produced it, while
/// the unit is still in that core's cache.
///
/// The pipeline calls `on_worker_chunk` on the worker, after the row-local
/// chain has produced a unit's chunk and before the chunk is published, and
/// `on_emit` on the consuming thread, as it returns that unit's chunk. The
/// chunk seen by the worker is the one the chain produced -- in particular its
/// Categorical columns still carry the unit's own dictionaries, which the
/// pipeline remaps onto a shared one only at emission. Publication orders the
/// two calls: whatever the worker stored is visible to `on_emit`.
///
/// Both calls are hints. A sink that stores nothing, or a consumer that ignores
/// what was stored, leaves the chunk to take its ordinary path.
class ScanWorkerSink {
   public:
    ScanWorkerSink() = default;
    ScanWorkerSink(const ScanWorkerSink&) = delete;
    ScanWorkerSink(ScanWorkerSink&&) = delete;
    auto operator=(const ScanWorkerSink&) -> ScanWorkerSink& = delete;
    auto operator=(ScanWorkerSink&&) -> ScanWorkerSink& = delete;
    virtual ~ScanWorkerSink() = default;

    /// On a pipeline worker. `unit` is the source unit's index.
    virtual void on_worker_chunk(std::size_t unit, const Chunk& chunk) noexcept = 0;

    /// On the consuming thread, just before the pipeline returns `chunk`.
    virtual void on_emit(std::size_t unit, const Chunk& chunk) noexcept = 0;
};

/// Offers `sink` to the next streamed scan pipeline built on this thread, for
/// the lifetime of the scope. A consumer opens one around building its input
/// only when that input is a map chain directly over a lazy scan, so the one
/// pipeline built inside it is the consumer's own.
class ScanWorkerSinkOffer {
   public:
    explicit ScanWorkerSinkOffer(std::shared_ptr<ScanWorkerSink> sink);
    ~ScanWorkerSinkOffer();
    ScanWorkerSinkOffer(const ScanWorkerSinkOffer&) = delete;
    ScanWorkerSinkOffer(ScanWorkerSinkOffer&&) = delete;
    auto operator=(const ScanWorkerSinkOffer&) -> ScanWorkerSinkOffer& = delete;
    auto operator=(ScanWorkerSinkOffer&&) -> ScanWorkerSinkOffer& = delete;

   private:
    std::shared_ptr<ScanWorkerSink> previous_;
};

/// The sink on offer, if any, which it also withdraws: one pipeline takes it.
[[nodiscard]] auto take_offered_scan_worker_sink() -> std::shared_ptr<ScanWorkerSink>;

}  // namespace ibex::runtime
