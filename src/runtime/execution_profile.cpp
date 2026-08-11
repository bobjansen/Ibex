// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/worker_pool.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <map>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "execution_profile_internal.hpp"

namespace ibex::runtime {

struct ExecutionProfileEntry {
    std::uint64_t node_id = 0;
    std::string label;
    std::atomic<std::uint64_t> build_self_ns{0};
    std::atomic<std::uint64_t> next_self_ns{0};
    std::atomic<std::uint64_t> pool_next_ns{0};
    std::atomic<std::uint64_t> source_self_ns{0};
    std::atomic<std::uint64_t> span_ns{0};
    std::atomic<std::uint64_t> pool_work_ns{0};
    std::atomic<std::uint64_t> calls{0};
    std::atomic<std::uint64_t> chunks{0};
    std::atomic<std::uint64_t> rows{0};
    std::atomic<std::uint64_t> pool_thread_calls{0};
    std::atomic<std::uint64_t> pool_tasks{0};
};

struct ExecutionProfileState::Impl {
    Impl(std::size_t budget, bool should_report)
        : worker_budget(budget == 0 ? 1 : budget), report(should_report) {}

    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    std::size_t worker_budget = 1;
    bool report = true;
    mutable std::mutex mutex;
    std::map<std::pair<std::uint64_t, std::string>, std::unique_ptr<ExecutionProfileEntry>> entries;
};

namespace {

thread_local ExecutionProfileScope::Frame* current_frame = nullptr;

[[nodiscard]] auto ns_since(std::chrono::steady_clock::time_point start) -> std::uint64_t {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now() - start)
                                          .count());
}

[[nodiscard]] auto node_kind_name(ir::NodeKind kind) -> std::string_view {
    using enum ir::NodeKind;
    switch (kind) {
        case Scan:
            return "scan";
        case Filter:
            return "filter";
        case Project:
            return "project";
        case Distinct:
            return "distinct";
        case Order:
            return "order";
        case Head:
            return "head";
        case Tail:
            return "tail";
        case Aggregate:
            return "aggregate";
        case Update:
            return "update";
        case Rename:
            return "rename";
        case Window:
            return "window";
        case Resample:
            return "resample";
        case AsTimeframe:
            return "as_timeframe";
        case Ascribe:
            return "ascribe";
        case Columns:
            return "columns";
        case ExternCall:
            return "extern";
        case Join:
            return "join";
        case Melt:
            return "melt";
        case Dcast:
            return "dcast";
        case Stream:
            return "stream";
        case Construct:
            return "construct";
        case Program:
            return "program";
        case Cov:
            return "cov";
        case Corr:
            return "corr";
        case Transpose:
            return "transpose";
        case Matmul:
            return "matmul";
        case Rbind:
            return "rbind";
        case Model:
            return "model";
        case FilterProject:
            return "filter_project";
        case FilterUpdateProject:
            return "filter_update_project";
        case FilterHead:
            return "filter_head";
        case FilterTail:
            return "filter_tail";
        case TopK:
            return "topk";
    }
    return "unknown";
}

[[nodiscard]] auto join_kind_name(ir::JoinKind kind) -> std::string_view {
    using enum ir::JoinKind;
    switch (kind) {
        case Inner:
            return "inner";
        case Left:
            return "left";
        case Right:
            return "right";
        case Outer:
            return "outer";
        case Semi:
            return "semi";
        case Anti:
            return "anti";
        case Cross:
            return "cross";
        case Asof:
            return "asof";
    }
    return "unknown";
}

[[nodiscard]] auto node_label(const ir::Node& node) -> std::string {
    if (node.kind() == ir::NodeKind::Scan) {
        const auto& scan = static_cast<const ir::ScanNode&>(node);
        return fmt::format("scan {}", scan.source_name());
    }
    if (node.kind() == ir::NodeKind::ExternCall) {
        const auto& call = static_cast<const ir::ExternCallNode&>(node);
        return fmt::format("extern {}", call.callee());
    }
    if (node.kind() == ir::NodeKind::Join) {
        const auto& join = static_cast<const ir::JoinNode&>(node);
        return fmt::format("join {} keys={}", join_kind_name(join.kind()), join.keys().size());
    }
    if (node.kind() == ir::NodeKind::Aggregate) {
        const auto& aggregate = static_cast<const ir::AggregateNode&>(node);
        return fmt::format("aggregate keys={} aggs={}", aggregate.group_by().size(),
                           aggregate.aggregations().size());
    }
    return std::string(node_kind_name(node.kind()));
}

class ProfiledOperator final : public Operator {
   public:
    ProfiledOperator(OperatorPtr child, std::shared_ptr<ExecutionProfileState> profile,
                     ExecutionProfileEntry* entry)
        : child_(std::move(child)), profile_(std::move(profile)), entry_(entry) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        entry_->calls.fetch_add(1, std::memory_order_relaxed);
        if (on_worker_pool_thread()) {
            entry_->pool_thread_calls.fetch_add(1, std::memory_order_relaxed);
        }
        ExecutionProfileScope scope(entry_, ProfilePhase::Next);
        auto result = child_->next();
        if (result.has_value() && result->has_value()) {
            entry_->chunks.fetch_add(1, std::memory_order_relaxed);
            entry_->rows.fetch_add((*result)->rows(), std::memory_order_relaxed);
        }
        return result;
    }

   private:
    OperatorPtr child_;
    // Owns every entry until the wrapped operator is destroyed.
    std::shared_ptr<ExecutionProfileState> profile_;
    ExecutionProfileEntry* entry_;
};

}  // namespace

struct ExecutionProfileScope::Frame {
    ExecutionProfileEntry* entry = nullptr;
    Frame* parent = nullptr;
    std::uint64_t child_ns = 0;
};

ExecutionProfileState::ExecutionProfileState(std::size_t worker_budget, bool report)
    : impl_(std::make_unique<Impl>(worker_budget, report)) {}

ExecutionProfileState::~ExecutionProfileState() {
    if (!impl_->report) {
        return;
    }
    const double total_ms = static_cast<double>(ns_since(impl_->start)) / 1.0e6;
    std::vector<ExecutionProfileEntry*> rows;
    {
        const std::lock_guard lock(impl_->mutex);
        rows.reserve(impl_->entries.size());
        for (const auto& [_, entry] : impl_->entries) {
            rows.push_back(entry.get());
        }
    }
    std::ranges::sort(rows, [](const auto* a, const auto* b) {
        const auto cost = [](const auto* entry) {
            return entry->build_self_ns.load(std::memory_order_relaxed) +
                   entry->next_self_ns.load(std::memory_order_relaxed) +
                   entry->source_self_ns.load(std::memory_order_relaxed);
        };
        return cost(a) > cost(b);
    });
    const std::size_t budget = impl_->worker_budget;
    const auto summary = summarize_execution_profile(snapshot(), total_ms, budget);
    fmt::print(stderr,
               "operator profile: wall_ms={:.3f} entries={} workers={} self_ms={:.3f} "
               "serial_self_ms={:.3f} serial_fraction={:.3f} amdahl_ceiling={:.2f}x "
               "pool_work_ms={:.3f} occupancy={:.3f}\n",
               total_ms, rows.size(), budget, summary.self_ms, summary.serial_self_ms,
               summary.serial_fraction, summary.amdahl_ceiling, summary.pool_work_ms,
               summary.occupancy);
    for (const auto* row : rows) {
        ExecutionProfileSnapshotRow occupancy_row;
        occupancy_row.span_ns = row->span_ns.load(std::memory_order_relaxed);
        occupancy_row.pool_work_ns = row->pool_work_ns.load(std::memory_order_relaxed);
        const double row_occupancy = profile_row_occupancy(occupancy_row, budget);
        fmt::print(stderr,
                   "profile node={} op=\"{}\" build_self_ms={:.3f} next_self_ms={:.3f} "
                   "source_self_ms={:.3f} span_ms={:.3f} pool_next_ms={:.3f} "
                   "pool_work_ms={:.3f} occupancy={:.3f} calls={} "
                   "chunks={} rows={} pool_calls={} pool_tasks={}\n",
                   row->node_id, row->label,
                   static_cast<double>(row->build_self_ns.load(std::memory_order_relaxed)) / 1.0e6,
                   static_cast<double>(row->next_self_ns.load(std::memory_order_relaxed)) / 1.0e6,
                   static_cast<double>(row->source_self_ns.load(std::memory_order_relaxed)) / 1.0e6,
                   static_cast<double>(row->span_ns.load(std::memory_order_relaxed)) / 1.0e6,
                   static_cast<double>(row->pool_next_ns.load(std::memory_order_relaxed)) / 1.0e6,
                   static_cast<double>(row->pool_work_ns.load(std::memory_order_relaxed)) / 1.0e6,
                   row_occupancy, row->calls.load(std::memory_order_relaxed),
                   row->chunks.load(std::memory_order_relaxed),
                   row->rows.load(std::memory_order_relaxed),
                   row->pool_thread_calls.load(std::memory_order_relaxed),
                   row->pool_tasks.load(std::memory_order_relaxed));
    }
}

auto profile_row_occupancy(const ExecutionProfileSnapshotRow& row, std::size_t workers) -> double {
    const std::size_t budget = workers == 0 ? 1 : workers;
    if (row.span_ns == 0) {
        return 0.0;
    }
    return static_cast<double>(row.pool_work_ns) /
           (static_cast<double>(row.span_ns) * static_cast<double>(budget));
}

auto summarize_execution_profile(const std::vector<ExecutionProfileSnapshotRow>& rows,
                                 double wall_ms, std::size_t workers) -> ExecutionProfileSummary {
    const std::size_t budget = workers == 0 ? 1 : workers;
    std::uint64_t self_total_ns = 0;
    std::uint64_t serial_self_ns = 0;
    std::uint64_t pool_total_ns = 0;
    for (const auto& row : rows) {
        const std::uint64_t self = row.build_self_ns + row.next_self_ns + row.source_self_ns;
        self_total_ns += self;
        pool_total_ns += row.pool_work_ns;
        if (row.pool_work_ns == 0) {
            serial_self_ns += self;  // this operator was handed no worker at all
        }
    }
    ExecutionProfileSummary summary;
    summary.self_ms = static_cast<double>(self_total_ns) / 1.0e6;
    summary.serial_self_ms = static_cast<double>(serial_self_ns) / 1.0e6;
    summary.pool_work_ms = static_cast<double>(pool_total_ns) / 1.0e6;
    summary.serial_fraction = self_total_ns == 0 ? 0.0
                                                 : static_cast<double>(serial_self_ns) /
                                                       static_cast<double>(self_total_ns);
    summary.amdahl_ceiling = summary.serial_fraction <= 0.0 ? 0.0 : 1.0 / summary.serial_fraction;
    summary.occupancy =
        wall_ms <= 0.0 ? 0.0 : summary.pool_work_ms / (wall_ms * static_cast<double>(budget));
    return summary;
}

auto ExecutionProfileState::entry(std::uint64_t node_id, std::string label)
    -> ExecutionProfileEntry* {
    const std::lock_guard lock(impl_->mutex);
    auto key = std::pair{node_id, label};
    auto [it, inserted] = impl_->entries.try_emplace(key);
    if (inserted) {
        it->second = std::make_unique<ExecutionProfileEntry>();
        it->second->node_id = node_id;
        it->second->label = std::move(label);
    }
    return it->second.get();
}

auto ExecutionProfileState::stage(std::string_view label) -> ExecutionProfileEntry* {
    return entry(0, std::string(label));
}

auto ExecutionProfileState::snapshot() const -> std::vector<ExecutionProfileSnapshotRow> {
    const std::lock_guard lock(impl_->mutex);
    std::vector<ExecutionProfileSnapshotRow> out;
    out.reserve(impl_->entries.size());
    for (const auto& [_, row] : impl_->entries) {
        out.push_back(ExecutionProfileSnapshotRow{
            .node_id = row->node_id,
            .label = row->label,
            .build_self_ns = row->build_self_ns.load(std::memory_order_relaxed),
            .next_self_ns = row->next_self_ns.load(std::memory_order_relaxed),
            .source_self_ns = row->source_self_ns.load(std::memory_order_relaxed),
            .span_ns = row->span_ns.load(std::memory_order_relaxed),
            .pool_next_ns = row->pool_next_ns.load(std::memory_order_relaxed),
            .pool_work_ns = row->pool_work_ns.load(std::memory_order_relaxed),
            .calls = row->calls.load(std::memory_order_relaxed),
            .chunks = row->chunks.load(std::memory_order_relaxed),
            .rows = row->rows.load(std::memory_order_relaxed),
            .pool_thread_calls = row->pool_thread_calls.load(std::memory_order_relaxed),
            .pool_tasks = row->pool_tasks.load(std::memory_order_relaxed),
        });
    }
    return out;
}

ExecutionProfileScope::ExecutionProfileScope(ExecutionProfileEntry* entry, ProfilePhase phase)
    : entry_(entry),
      phase_(phase),
      start_(entry == nullptr ? std::chrono::steady_clock::time_point{}
                              : std::chrono::steady_clock::now()) {
    if (entry_ == nullptr) {
        return;
    }
    frame_ = std::make_unique<Frame>();
    frame_->entry = entry_;
    frame_->parent = current_frame;
    current_frame = frame_.get();
}

ExecutionProfileScope::~ExecutionProfileScope() {
    if (entry_ == nullptr) {
        return;
    }
    const std::uint64_t elapsed = ns_since(start_);
    const std::uint64_t self = elapsed >= frame_->child_ns ? elapsed - frame_->child_ns : 0;
    if (phase_ == ProfilePhase::Build) {
        entry_->build_self_ns.fetch_add(self, std::memory_order_relaxed);
    } else if (phase_ == ProfilePhase::Source) {
        entry_->source_self_ns.fetch_add(self, std::memory_order_relaxed);
    } else {
        if (on_worker_pool_thread()) {
            entry_->pool_next_ns.fetch_add(self, std::memory_order_relaxed);
        } else {
            entry_->next_self_ns.fetch_add(self, std::memory_order_relaxed);
            entry_->span_ns.fetch_add(elapsed, std::memory_order_relaxed);
        }
    }
    current_frame = frame_->parent;
    if (current_frame != nullptr) {
        current_frame->child_ns += elapsed;
    }
}

auto profile_operator(OperatorPtr op, std::shared_ptr<ExecutionProfileState> profile,
                      const ir::Node& node) -> OperatorPtr {
    if (profile == nullptr) {
        return op;
    }
    auto* entry = profile->entry(node.id().value, node_label(node));
    return std::make_unique<ProfiledOperator>(std::move(op), std::move(profile), entry);
}

auto execution_profile_entry(const std::shared_ptr<ExecutionProfileState>& profile,
                             const ir::Node& node) -> ExecutionProfileEntry* {
    return profile == nullptr ? nullptr : profile->entry(node.id().value, node_label(node));
}

auto current_execution_profile_entry() noexcept -> ExecutionProfileEntry* {
    return current_frame == nullptr ? nullptr : current_frame->entry;
}

void record_execution_profile_worker(ExecutionProfileEntry* entry,
                                     std::chrono::nanoseconds elapsed) noexcept {
    if (entry == nullptr) {
        return;
    }
    entry->pool_work_ns.fetch_add(static_cast<std::uint64_t>(elapsed.count()),
                                  std::memory_order_relaxed);
    entry->pool_tasks.fetch_add(1, std::memory_order_relaxed);
}

auto execution_profile_requested() noexcept -> bool {
    static const bool enabled = std::getenv("IBEX_PROFILE_OPERATORS") != nullptr;
    return enabled;
}

}  // namespace ibex::runtime
