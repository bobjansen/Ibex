// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <memory>
#include <vector>

namespace ibex::runtime {

/// A Categorical column the pipeline emitted still over its unit's OWN
/// dictionary, because the consumer asked to translate it itself: `shared` is
/// the shared dictionary (no rows) the pipeline would have remapped it onto,
/// and `remap[local code]` the shared code.
struct DeferredCategoricalRemap {
    std::size_t column = 0;
    Column<Categorical> shared;
    std::vector<Column<Categorical>::code_type> remap;
};

/// `local`'s rows over `deferred.shared`: the per-row translation the pipeline
/// skipped.
[[nodiscard]] auto translate_deferred_categorical(const Column<Categorical>& local,
                                                  const DeferredCategoricalRemap& deferred)
    -> Column<Categorical>;

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
///
/// The one exception is `untranslated_columns`: a Categorical column the sink
/// names there is emitted over the unit's own dictionary, and `on_emit` is
/// handed the remap instead. Accepting it (returning true) makes the consumer
/// responsible for translating that column before anything reads its codes as
/// shared ones; refusing it makes the pipeline translate after all.
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

    /// On the consuming thread, before the pipeline remaps `chunk`'s
    /// Categorical columns: those whose per-row translation the consumer will
    /// do itself, if it needs it at all. Only a Categorical column without
    /// validity is deferred; any other index is ignored.
    [[nodiscard]] virtual auto untranslated_columns(std::size_t /*unit*/,
                                                    const Chunk& /*chunk*/) noexcept
        -> std::vector<std::size_t> {
        return {};
    }

    /// On the consuming thread, just before the pipeline returns `chunk`, with
    /// the remaps of the columns left untranslated. False refuses them: the
    /// pipeline then translates those columns itself.
    [[nodiscard]] virtual auto on_emit(std::size_t unit, const Chunk& chunk,
                                       std::vector<DeferredCategoricalRemap> deferred) noexcept
        -> bool = 0;
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
