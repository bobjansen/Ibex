// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_filter.hpp"

#include <bit>
#include <ranges>
#include <type_traits>

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "kernel_gather.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime::kernel {

namespace {

auto predicate_input(const ChunkView& view) -> PredicateInput {
    return PredicateInput{
        &view,
        [](const void* state) noexcept { return static_cast<const ChunkView*>(state)->rows(); },
        [](const void* state, const std::string& name) noexcept -> const ColumnEntry* {
            const auto& input = *static_cast<const ChunkView*>(state);
            const auto position = input.find_column(name);
            return position.has_value() ? &input.entry(*position) : nullptr;
        }};
}

auto is_chunk_predicate_native(const ir::Expr& expr) -> bool {
    return std::visit(
        [](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::ColumnRef> || std::is_same_v<T, ir::Literal>) {
                return true;
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr> ||
                                 std::is_same_v<T, ir::CompareExpr>) {
                return is_chunk_predicate_native(*node.left) &&
                       is_chunk_predicate_native(*node.right);
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                return is_chunk_predicate_native(*node.left) &&
                       (node.right == nullptr || is_chunk_predicate_native(*node.right));
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return is_chunk_predicate_native(*node.operand);
            } else {
                return false;
            }
        },
        expr.node);
}

struct FilterChunkOutputLayout {
    Chunk output;
    std::vector<std::size_t> src_of_dst;
};

auto build_filter_chunk_output_layout(const ChunkView& input,
                                      const std::vector<ir::ColumnRef>* project)
    -> std::expected<FilterChunkOutputLayout, std::string> {
    FilterChunkOutputLayout layout;
    if (project == nullptr) {
        layout.output.columns.reserve(input.columns());
        layout.src_of_dst.reserve(input.columns());
        for (std::size_t i = 0; i < input.columns(); ++i) {
            const auto& entry = input.entry(i);
            layout.output.add_column(entry.name, make_empty_like(*entry.column));
            layout.src_of_dst.push_back(i);
        }
        return layout;
    }
    layout.output.columns.reserve(project->size());
    layout.src_of_dst.reserve(project->size());
    for (const auto& col : *project) {
        const auto position = input.find_column(col.name);
        if (!position.has_value()) {
            return std::unexpected("select column not found: " + col.name);
        }
        const auto& entry = input.entry(*position);
        layout.output.add_column(entry.name, make_empty_like(*entry.column));
        layout.src_of_dst.push_back(*position);
    }
    return layout;
}

auto chunk_filter_selection(const FilterSelection& selection, std::size_t row_base) -> Selection {
    return Selection{RowWordBlocks{.words = selection.keep_words.data(),
                                   .word_count = selection.keep_words.size(),
                                   .row_base = row_base}};
}

void count_selected_chunk_chars(const ChunkView& input, const std::vector<std::size_t>& src_of_dst,
                                const FilterSelection& selection, std::size_t row_base,
                                std::vector<std::size_t>& chars) {
    for (std::size_t d = 0; d < src_of_dst.size(); ++d) {
        const auto* src = std::get_if<Column<std::string>>(input.entry(src_of_dst[d]).column.get());
        if (src == nullptr) {
            continue;
        }
        const auto* offsets = src->offsets_data();
        for (std::size_t w = 0; w < selection.keep_words.size(); ++w) {
            std::uint64_t bits = selection.keep_words[w];
            const std::size_t word_base = row_base + (w * 64);
            while (bits != 0) {
                const auto bit = static_cast<std::size_t>(std::countr_zero(bits));
                const std::size_t row = word_base + bit;
                chars[d] += offsets[row + 1] - offsets[row];
                bits &= bits - 1;
            }
        }
    }
}

void presize_filter_chunk_output(Chunk& output, const ChunkView& input,
                                 const std::vector<std::size_t>& src_of_dst, std::size_t rows_total,
                                 const std::vector<std::size_t>& chars_total) {
    for (std::size_t d = 0; d < output.columns.size(); ++d) {
        std::visit(
            [&](auto& dst) {
                using ColT = std::decay_t<decltype(dst)>;
                if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    dst.resize_for_gather(rows_total, chars_total[d]);
                    dst.offsets_data()[0] = 0;
                } else if constexpr (std::is_same_v<ColT, Column<bool>> ||
                                     std::is_same_v<ColT, Column<Categorical>>) {
                    dst.resize(rows_total);
                } else if constexpr (std::is_trivially_default_constructible_v<
                                         typename ColT::value_type>) {
                    dst.resize_for_overwrite(rows_total);
                } else {
                    dst.resize(rows_total);
                }
            },
            *output.columns[d].column);
        if (input.entry(src_of_dst[d]).validity.has_value()) {
            output.columns[d].validity.emplace(rows_total, false);
        }
    }
}

void gather_chunk_selection_into(Chunk& output, const ChunkView& input,
                                 const std::vector<std::size_t>& src_of_dst,
                                 const FilterSelection& selection, std::size_t row_base) {
    const Selection selected = chunk_filter_selection(selection, row_base);
    for (std::size_t d = 0; d < output.columns.size(); ++d) {
        const auto& src_entry = input.entry(src_of_dst[d]);
        auto& dst_entry = output.columns[d];
        std::visit(
            [&](const auto& src) {
                using ColT = std::decay_t<decltype(src)>;
                auto* out = std::get_if<ColT>(dst_entry.column.get());
                if (out == nullptr) {
                    invariant_violation(
                        "filter_chunk: source/destination gather column type mismatch");
                }
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    using Code = Column<Categorical>::code_type;
                    gather_selected(
                        ColumnView<Code>(src.codes_data(), src.size(), nullptr), selected,
                        OutputSpan<Code>{
                            .data = out->codes_data(), .begin = 0, .count = selection.kept});
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    gather_selected_strings(StringView{.offsets = src.offsets_data(),
                                                       .chars = src.chars_data(),
                                                       .rows = src.size()},
                                            selected,
                                            StringOutputSpan{.offsets = out->offsets_data(),
                                                             .chars = out->chars_data(),
                                                             .begin = 0,
                                                             .count = selection.kept,
                                                             .char_base = 0});
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    gather_selected_bool(
                        BoolView(src), selected,
                        BoolOutputSpan{
                            .words = out->words_data(), .begin = 0, .count = selection.kept});
                } else {
                    using T = ColT::value_type;
                    gather_selected(
                        ColumnView<T>(src.data(), src.size(), nullptr), selected,
                        OutputSpan<T>{.data = out->data(), .begin = 0, .count = selection.kept});
                }
            },
            *src_entry.column);
        if (src_entry.validity.has_value()) {
            gather_selected_validity(ValidityView(*src_entry.validity), selected,
                                     BoolOutputSpan{.words = dst_entry.validity->words_data(),
                                                    .begin = 0,
                                                    .count = selection.kept});
        }
    }
}

auto filter_chunk_selection(const ChunkView& input, const FilterSelection& selection,
                            const std::vector<ir::ColumnRef>* project, std::size_t row_base)
    -> std::expected<Chunk, std::string> {
    auto layout = build_filter_chunk_output_layout(input, project);
    if (!layout) {
        return std::unexpected(std::move(layout.error()));
    }
    std::vector<std::size_t> chars(layout->output.columns.size(), 0);
    count_selected_chunk_chars(input, layout->src_of_dst, selection, row_base, chars);
    presize_filter_chunk_output(layout->output, input, layout->src_of_dst, selection.kept, chars);
    gather_chunk_selection_into(layout->output, input, layout->src_of_dst, selection, row_base);
    if (layout->output.columns.empty()) {
        layout->output.logical_rows = selection.kept;
    }
    layout->output.set_properties(TableProperties::derive(
        input.properties(),
        [&](const std::string& name) -> KeyFate {
            return project == nullptr || std::ranges::any_of(layout->output.columns,
                                                             [&](const ColumnEntry& entry) {
                                                                 return entry.name == name;
                                                             })
                       ? KeyFate::kept(name)
                       : KeyFate::dropped();
        },
        RowTransform::Subset));
    return std::move(layout->output);
}

auto chunk_from_filtered(std::expected<Table, std::string> filtered, std::uint64_t sequence,
                         std::size_t row_offset) -> std::expected<Chunk, std::string> {
    if (!filtered.has_value()) {
        return std::unexpected(std::move(filtered.error()));
    }
    Chunk output = table_to_chunk(std::move(filtered.value()));
    output.sequence = sequence;
    output.row_offset = row_offset;
    return output;
}

auto filter_chunk_range_native(Chunk input, const ir::Expr& predicate,
                               const std::vector<ir::ColumnRef>* project, std::size_t row_limit,
                               const ScalarRegistry* scalars) -> std::expected<Chunk, std::string> {
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    const ChunkView view(input);
    const std::size_t rows = view.rows();
    auto selection = compute_filter_selection(predicate_input(view), predicate, scalars,
                                              ::ibex::runtime::RowRange::whole(rows), row_limit);
    if (!selection) {
        return std::unexpected(std::move(selection.error()));
    }
    auto output = filter_chunk_selection(view, *selection, project, 0);
    if (!output) {
        return std::unexpected(std::move(output.error()));
    }
    output->sequence = sequence;
    output->row_offset = row_offset;
    return output;
}

}  // namespace

auto filter_chunk(Chunk input, const ir::Expr& predicate, const ScalarRegistry* scalars)
    -> std::expected<Chunk, std::string> {
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    if (is_chunk_predicate_native(predicate)) {
        return filter_chunk_range_native(std::move(input), predicate, nullptr, 0, scalars);
    }
    return chunk_from_filtered(filter_table(chunk_to_table(std::move(input)), predicate, scalars),
                               sequence, row_offset);
}

auto filter_project_chunk(Chunk input, const ir::Expr& predicate,
                          const std::vector<ir::ColumnRef>& columns, const ScalarRegistry* scalars)
    -> std::expected<Chunk, std::string> {
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    if (is_chunk_predicate_native(predicate)) {
        return filter_chunk_range_native(std::move(input), predicate, &columns, 0, scalars);
    }
    return chunk_from_filtered(
        filter_project_table(chunk_to_table(std::move(input)), predicate, columns, scalars),
        sequence, row_offset);
}

auto filter_limit_chunk(Chunk input, const ir::Expr& predicate, std::size_t row_limit,
                        const ScalarRegistry* scalars) -> std::expected<Chunk, std::string> {
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    if (is_chunk_predicate_native(predicate)) {
        return filter_chunk_range_native(std::move(input), predicate, nullptr, row_limit, scalars);
    }
    return chunk_from_filtered(
        filter_table_limit(chunk_to_table(std::move(input)), predicate, row_limit, scalars),
        sequence, row_offset);
}

auto project_chunk(Chunk input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Chunk, std::string> {
    const ChunkView view(input);
    std::vector<MappedChunkColumn> map;
    map.reserve(columns.size());
    for (const auto& column : columns) {
        if (column.name.empty()) {
            if (const auto& time_index = view.properties().time_index();
                time_index.has_value() && std::ranges::none_of(map, [&](const auto& mapped) {
                    return mapped.name == *time_index;
                })) {
                const auto position = view.find_column(*time_index);
                if (!position.has_value())
                    return std::unexpected("select column not found: " + *time_index);
                map.push_back({.source_position = *position, .name = *time_index});
            }
            continue;
        }
        const auto position = view.find_column(column.name);
        if (!position.has_value())
            return std::unexpected("select column not found: " + column.name);
        map.push_back({.source_position = *position, .name = column.name});
    }
    const auto properties = TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return std::ranges::any_of(map, [&](const auto& mapped) { return mapped.name == name; })
                       ? KeyFate::kept(name)
                       : KeyFate::dropped();
        },
        RowTransform::Preserve);
    return map_chunk(view, map, properties);
}

auto filter_update_project_chunk(Chunk input, const ir::Expr& predicate,
                                 const std::vector<ir::FieldSpec>& fields,
                                 const std::vector<ir::ColumnRef>& project_columns,
                                 const std::vector<ir::ColumnRef>& gather_columns,
                                 const ScalarRegistry* scalars, const ExternRegistry* externs,
                                 const ExecutionContext& exec)
    -> std::expected<Chunk, std::string> {
    auto filtered = filter_project_chunk(std::move(input), predicate, gather_columns, scalars);
    if (!filtered.has_value())
        return std::unexpected(std::move(filtered.error()));
    auto updated =
        update_row_local_chunk(std::move(filtered.value()), fields, scalars, externs, exec);
    if (!updated.has_value())
        return std::unexpected(std::move(updated.error()));
    return project_chunk(std::move(updated.value()), project_columns);
}

}  // namespace ibex::runtime::kernel
