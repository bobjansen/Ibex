// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/compiler.hpp>
#include <ibex/core/time_zone.hpp>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace ibex::detail {
/// Transparent hasher that accepts std::string, std::string_view, and const char*,
/// enabling robin_hood::unordered_map::find(string_view) without constructing a std::string.
struct StringHash {
    using is_transparent = void;
    auto operator()(std::string_view sv) const noexcept -> std::size_t {
        return std::hash<std::string_view>{}(sv);
    }
};

template <typename T>
class NoInitAllocator {
   public:
    using value_type = T;

    NoInitAllocator() noexcept = default;

    template <typename U>
    constexpr NoInitAllocator(const NoInitAllocator<U>& /*_*/) noexcept {}

    [[nodiscard]] auto allocate(std::size_t n) -> T* { return std::allocator<T>{}.allocate(n); }

    void deallocate(T* p, std::size_t n) noexcept { std::allocator<T>{}.deallocate(p, n); }

    template <typename U>
    void construct(U* p) noexcept(std::is_nothrow_default_constructible_v<U>) {
        if constexpr (!std::is_trivially_default_constructible_v<U>) {
            ::new (static_cast<void*>(p)) U();
        }
    }

    template <typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* p) noexcept {
        p->~U();
    }

    template <typename U>
    struct rebind {
        using other = NoInitAllocator<U>;
    };
};

template <typename T, typename U>
auto operator==(const NoInitAllocator<T>& /*_*/, const NoInitAllocator<U>& /*_*/) noexcept -> bool {
    return true;
}

template <typename T, typename U>
auto operator!=(const NoInitAllocator<T>& /*_*/, const NoInitAllocator<U>& /*_*/) noexcept -> bool {
    return false;
}

/// A vector whose `resize` does not value-initialize the new elements.
///
/// For row-sized buffers whose every element is written before it is read.
/// `std::vector::resize` fills the whole range first, and for a buffer sized by
/// row count that is a second full pass over the data. Callers owe the same
/// obligation as `Column::resize_for_overwrite`: write every slot, or read
/// garbage. Worth it only where the fill is a real fraction of the work — see
/// the note in `Mask`.
template <typename T>
using NoInitVector = std::vector<T, NoInitAllocator<T>>;
}  // namespace ibex::detail

namespace ibex {

/// Concept constraining valid column element types.
template <typename T>
concept ColumnElement = std::regular<T> && std::totally_ordered<T>;

/// Tag type for dictionary-encoded categorical columns.
struct Categorical {};

/// A typed contiguous column.
///
/// Ordinarily Column<T> owns a vector. It can also adopt an immutable external
/// buffer together with a shared lifetime owner (Arrow C Data is the first
/// caller). Reads stay zero-copy; the first mutable access detaches into owned
/// storage. That keeps the existing value-like API while making foreign,
/// sliced primitive arrays safe under the runtime's copy-on-write invariant.
template <typename T>
class Column {
   public:
    /// What these values MEAN (e.g. the time zone of a Timestamp column), as
    /// opposed to where the rows sit. It travels with the column: sharing,
    /// copying, or gathering rows out of it all preserve it, because none of
    /// those change what a value means. See `ColumnMeta`.
    [[nodiscard]] auto meta() const noexcept -> const ColumnMeta& { return meta_; }
    void set_meta(ColumnMeta meta) noexcept { meta_ = meta; }
    using value_type = T;
    using size_type = std::size_t;
    using storage_type = std::vector<T, detail::NoInitAllocator<T>>;
    using iterator = T*;
    using const_iterator = const T*;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    static_assert(ColumnElement<T>,
                  "Column<T> requires T to satisfy ColumnElement (regular + totally ordered).");
    static_assert(!std::is_same_v<T, std::string>,
                  "Use the Column<std::string> specialization (flat buffer).");

    Column() = default;

    explicit Column(std::vector<T> data)
        : data_(std::make_move_iterator(data.begin()), std::make_move_iterator(data.end())) {}

    Column(std::initializer_list<T> init) : data_(init) {}

    Column(const Column& other)
        : data_(other.is_external() ? storage_type{} : other.data_),
          external_owner_(other.external_owner_),
          external_data_(other.is_external() ? other.external_data_ : nullptr),
          external_offset_(other.is_external() ? other.external_offset_ : 0),
          external_size_(other.is_external() ? other.external_size_ : 0),
          meta_(other.meta_) {}

    Column(Column&& other) noexcept
        : data_(std::move(other.data_)),
          external_owner_(std::move(other.external_owner_)),
          external_data_(external_owner_ ? other.external_data_ : nullptr),
          external_offset_(external_owner_ ? other.external_offset_ : 0),
          external_size_(external_owner_ ? other.external_size_ : 0),
          meta_(other.meta_) {
        other.external_data_ = nullptr;
        other.external_offset_ = 0;
        other.external_size_ = 0;
    }

    auto operator=(const Column& other) -> Column& {
        if (this != &other) {
            Column copy(other);
            swap(copy);
        }
        return *this;
    }

    auto operator=(Column&& other) noexcept -> Column& {
        if (this != &other) {
            Column moved(std::move(other));
            swap(moved);
        }
        return *this;
    }

    /// Declared because the four copy/move members are: the rule of five is a
    /// readability rule here, not a correctness one — every member cleans up
    /// after itself, so the compiler-generated body is already right.
    ~Column() = default;

    void swap(Column& other) noexcept {
        data_.swap(other.data_);
        external_owner_.swap(other.external_owner_);
        std::swap(external_data_, other.external_data_);
        std::swap(external_offset_, other.external_offset_);
        std::swap(external_size_, other.external_size_);
        std::swap(meta_, other.meta_);
    }

    /// Adopt an immutable contiguous buffer. `owner` must keep every element in
    /// `[values, values + size)` alive. The pointer may already include an
    /// Arrow array's logical offset, so slices do not need rebasing.
    [[nodiscard]] static auto from_external(std::shared_ptr<const void> owner, const T* values,
                                            size_type size) -> Column {
        return from_external(std::move(owner), values, 0, size);
    }

    /// Adopt a slice of an immutable contiguous buffer while retaining its
    /// original element offset for Arrow C Data round trips.
    [[nodiscard]] static auto from_external(std::shared_ptr<const void> owner, const T* values,
                                            size_type offset, size_type size) -> Column {
        if (!owner) {
            throw std::invalid_argument("external column requires a lifetime owner");
        }
        if (values == nullptr && size != 0) {
            throw std::invalid_argument("external column requires a data buffer");
        }
        Column column;
        column.external_owner_ = std::move(owner);
        column.external_data_ = values == nullptr ? nullptr : values + offset;
        column.external_offset_ = offset;
        column.external_size_ = size;
        return column;
    }

    /// Whether reads currently refer to adopted storage rather than `data_`.
    [[nodiscard]] auto is_external() const noexcept -> bool {
        return static_cast<bool>(external_owner_);
    }

    /// Base storage pointer and logical element offset. Most callers should use
    /// `data()`; these accessors exist for zero-copy Arrow C Data export.
    [[nodiscard]] auto buffer_data() const noexcept -> const T* {
        if (!is_external()) {
            return data_.data();
        }
        return external_data_ != nullptr ? external_data_ - external_offset_ : external_data_;
    }
    [[nodiscard]] auto buffer_offset() const noexcept -> size_type {
        return is_external() ? external_offset_ : 0;
    }

    /// Number of elements.
    [[nodiscard]] auto size() const noexcept -> size_type {
        return is_external() ? external_size_ : data_.size();
    }

    /// Immutable element access (bounds-checked).
    [[nodiscard]] auto at(size_type idx) const -> const T& {
        if (idx >= size()) {
            throw std::out_of_range("Column::at");
        }
        return data_ptr()[idx];
    }

    /// Mutable element access (bounds-checked).
    [[nodiscard]] auto at(size_type idx) -> T& {
        detach_external();
        return data_.at(idx);
    }

    /// Unchecked element access.
    [[nodiscard]] auto operator[](size_type idx) const noexcept -> const T& {
        return data_ptr()[idx];
    }

    /// Unchecked mutable element access.
    [[nodiscard]] auto operator[](size_type idx) -> T& {
        detach_external();
        return data_[idx];
    }

    /// Zero-copy immutable view of the underlying data.
    [[nodiscard]] auto span() const noexcept -> std::span<const T> { return {data_ptr(), size()}; }

    /// Mutable view, detaching adopted storage first.
    [[nodiscard]] auto span() -> std::span<T> {
        detach_external();
        return {data_.data(), data_.size()};
    }

    /// Append a value.
    ///
    /// The copy-on-write check is spelled out here, rather than delegated to
    /// `detach_external()`, so the body stays small enough for the inliner —
    /// and then forced, because leaving it to the cost model was measurably not
    /// enough. Kernels append a value per row, and once this stops being
    /// inlined the call overhead alone costs more than the append: `cumsum`
    /// over 1M rows spent a third of its time here as an out-of-line call, and
    /// dropping the attribute moved the whole benchmark suite's geomean from
    /// 1.048x to 1.076x of the pre-adoption baseline.
    ///
    /// Prefer `reserve` + a hoisted `data()` pointer in a hot kernel regardless:
    /// that resolves the storage once instead of per row.
    IBEX_ALWAYS_INLINE void push_back(const T& value) {
        if (is_external()) [[unlikely]] {
            detach_external_slow();
        }
        data_.push_back(value);
    }
    IBEX_ALWAYS_INLINE void push_back(T&& value) {
        if (is_external()) [[unlikely]] {
            detach_external_slow();
        }
        data_.push_back(std::move(value));
    }

    /// Construct a value in-place.
    template <typename... Args>
        requires std::constructible_from<T, Args...>
    auto emplace_back(Args&&... args) -> T& {
        detach_external();
        data_.emplace_back(std::forward<Args>(args)...);
        return data_.back();
    }

    auto emplace_back() -> T& {
        detach_external();
        data_.emplace_back(T{});
        return data_.back();
    }

    /// Assign from count and value.
    void assign(size_type count, const T& value) {
        drop_external();
        data_.assign(count, value);
    }

    /// Assign from range.
    template <typename InputIt>
        requires std::input_iterator<InputIt>
    void assign(InputIt first, InputIt last) {
        storage_type assigned(first, last);
        drop_external();
        data_ = std::move(assigned);
    }
    /// Assign from initializer list.
    void assign(std::initializer_list<T> init) {
        drop_external();
        data_.assign(init);
    }

    /// Insert value before position.
    [[nodiscard]] auto insert(const_iterator pos, const T& value) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), value);
        return iterator_at(offset);
    }
    [[nodiscard]] auto insert(const_iterator pos, T&& value) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), std::move(value));
        return iterator_at(offset);
    }

    /// Insert count copies of value.
    [[nodiscard]] auto insert(const_iterator pos, size_type count, const T& value) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), count, value);
        return iterator_at(offset);
    }

    /// Insert range.
    template <typename InputIt>
        requires std::input_iterator<InputIt>
    [[nodiscard]] auto insert(const_iterator pos, InputIt first, InputIt last) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), first, last);
        return iterator_at(offset);
    }

    /// Insert initializer list.
    [[nodiscard]] auto insert(const_iterator pos, std::initializer_list<T> init) -> iterator {
        return insert(pos, init.begin(), init.end());
    }

    /// Emplace value before position.
    template <typename... Args>
        requires std::constructible_from<T, Args...>
    [[nodiscard]] auto emplace(const_iterator pos, Args&&... args) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.emplace(data_.begin() + static_cast<std::ptrdiff_t>(offset),
                      std::forward<Args>(args)...);
        return iterator_at(offset);
    }

    /// Erase element at position.
    [[nodiscard]] auto erase(const_iterator pos) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.erase(data_.begin() + static_cast<std::ptrdiff_t>(offset));
        const auto next = std::min(offset, data_.size());
        return iterator_at(next);
    }
    /// Erase range.
    [[nodiscard]] auto erase(const_iterator first, const_iterator last) -> iterator {
        const auto first_offset = iterator_offset(first);
        const auto last_offset = iterator_offset(last);
        detach_external();
        data_.erase(data_.begin() + static_cast<std::ptrdiff_t>(first_offset),
                    data_.begin() + static_cast<std::ptrdiff_t>(last_offset));
        const auto next = std::min(first_offset, data_.size());
        return iterator_at(next);
    }

    /// Reserve capacity.
    void reserve(size_type capacity) {
        detach_external();
        data_.reserve(capacity);
    }

    /// Current capacity.
    [[nodiscard]] auto capacity() const noexcept -> size_type {
        return is_external() ? external_size_ : data_.capacity();
    }

    /// Remove all elements.
    void clear() noexcept {
        drop_external();
        data_.clear();
    }

    /// Resize the column (value-initialize new elements).
    void resize(size_type count) {
        detach_external();
        data_.resize(count, T{});
    }

    /// Resize the column (copy-initialize new elements with value).
    void resize(size_type count, const T& value) {
        detach_external();
        data_.resize(count, value);
    }

    /// Resize without value-initializing new slots. Callers must overwrite every element.
    void resize_for_overwrite(size_type count)
        requires std::is_trivially_default_constructible_v<T>
    {
        // Existing values must survive `resize` when count is smaller or when
        // the caller grows relative to an adopted slice.
        detach_external();
        data_.resize(count);
    }

    /// Reduce capacity to fit size.
    void shrink_to_fit() {
        detach_external();
        data_.shrink_to_fit();
    }

    /// Remove the last element.
    void pop_back() {
        detach_external();
        data_.pop_back();
    }

    /// Whether the column is empty.
    [[nodiscard]] auto empty() const noexcept -> bool { return size() == 0; }

    /// Max size supported by the allocator.
    [[nodiscard]] auto max_size() const noexcept -> size_type { return data_.max_size(); }

    /// Raw data access.
    [[nodiscard]] auto data() -> T* {
        detach_external();
        return data_.data();
    }
    [[nodiscard]] auto data() const noexcept -> const T* { return data_ptr(); }

    /// First and last elements.
    [[nodiscard]] auto front() const -> const T& { return *data_ptr(); }
    [[nodiscard]] auto front() -> T& {
        detach_external();
        return data_.front();
    }
    [[nodiscard]] auto back() const -> const T& { return data_ptr()[size() - 1]; }
    [[nodiscard]] auto back() -> T& {
        detach_external();
        return data_.back();
    }

    /// Apply a predicate and return a filtered column.
    template <std::predicate<const T&> Pred>
    [[nodiscard]] auto filter(Pred pred) const -> Column<T> {
        std::vector<T> result;
        std::ranges::copy_if(span(), std::back_inserter(result), pred);
        return Column<T>{std::move(result)};
    }

    /// Apply a transform and return a new column.
    template <typename F>
        requires std::invocable<F, const T&>
    [[nodiscard]] auto transform(F func) const -> Column<std::invoke_result_t<F, const T&>> {
        using U = std::invoke_result_t<F, const T&>;
        std::vector<U> result;
        result.reserve(size());
        std::ranges::transform(span(), std::back_inserter(result), func);
        return Column<U>{std::move(result)};
    }

    // Iterator support
    [[nodiscard]] auto begin() -> iterator {
        detach_external();
        return data_.data();
    }
    [[nodiscard]] auto end() -> iterator {
        detach_external();
        return iterator_at(data_.size());
    }
    [[nodiscard]] auto begin() const noexcept -> const_iterator { return data_ptr(); }
    [[nodiscard]] auto end() const noexcept -> const_iterator {
        return empty() ? data_ptr() : data_ptr() + size();
    }
    [[nodiscard]] auto cbegin() const noexcept -> const_iterator { return begin(); }
    [[nodiscard]] auto cend() const noexcept -> const_iterator { return end(); }
    [[nodiscard]] auto rbegin() -> reverse_iterator { return reverse_iterator{end()}; }
    [[nodiscard]] auto rend() -> reverse_iterator { return reverse_iterator{begin()}; }
    [[nodiscard]] auto rbegin() const noexcept -> const_reverse_iterator {
        return const_reverse_iterator{end()};
    }
    [[nodiscard]] auto rend() const noexcept -> const_reverse_iterator {
        return const_reverse_iterator{begin()};
    }
    [[nodiscard]] auto crbegin() const noexcept -> const_reverse_iterator { return rbegin(); }
    [[nodiscard]] auto crend() const noexcept -> const_reverse_iterator { return rend(); }

   private:
    /// Reads resolve the storage in use rather than reading a mirror kept in
    /// step by every mutation. The branch is on a `shared_ptr` null check that
    /// is constant for a column's whole lifetime, so it predicts perfectly;
    /// eagerly mirroring instead cost three stores inside `push_back`, which
    /// made appending to an owned column several times slower and dominated
    /// profiles of every kernel that builds its output that way.
    [[nodiscard]] auto data_ptr() const noexcept -> const T* {
        return is_external() ? external_data_ : data_.data();
    }

    [[nodiscard]] auto iterator_offset(const_iterator pos) const noexcept -> size_type {
        const auto* first = data_ptr();
        if (pos == first) {
            return 0;
        }
        return static_cast<size_type>(pos - first);
    }

    void drop_external() noexcept {
        external_owner_.reset();
        external_offset_ = 0;
    }

    /// Copy-on-write hook, called by every mutating accessor.
    ///
    /// Split so the common case — an already-owned column — is a single
    /// predictable branch the compiler will inline, with the copy out of line.
    /// This matters more than it looks: mutable `operator[]` calls this, and a
    /// kernel writing `result[i]` in a row loop was paying a real (unvectorized)
    /// call per element once Arrow buffer adoption made the check necessary at
    /// all. The optimizer cannot hoist the check itself, because the call it
    /// guards may change the very state it tests.
    void detach_external() {
        if (!is_external()) {
            return;
        }
        detach_external_slow();
    }

    // NOLINTNEXTLINE(readability-function-size)
    IBEX_NOINLINE void detach_external_slow() {
        storage_type owned;
        owned.reserve(external_size_);
        if (external_size_ != 0) {
            owned.insert(owned.end(), external_data_, external_data_ + external_size_);
        }
        data_ = std::move(owned);
        drop_external();
    }

    /// Only ever called after `detach_external()`, so the storage is `data_`.
    [[nodiscard]] auto iterator_at(size_type offset) noexcept -> iterator {
        return data_.empty() ? data_.data() : data_.data() + offset;
    }

    storage_type data_;
    std::shared_ptr<const void> external_owner_;
    const T* external_data_ = nullptr;
    size_type external_offset_ = 0;
    size_type external_size_ = 0;

    /// See `meta()`. Declared last so the hand-written constructors can append
    /// it to their member-init lists without tripping -Wreorder. Four bytes,
    /// and trivially copyable -- deliberately, since a column is copied per
    /// chunk in the chunked paths.
    ColumnMeta meta_;
};

/// Fraction of a column's rows that may be distinct before an importer stops
/// promoting it to `Column<Categorical>`.
inline constexpr double kCategoricalPromotionRatio = 0.10;

/// Distinct-value budget for promoting a null-free string column to
/// Categorical, given its row count. Every importer (read_csv, the JSON
/// table builder its plugins share) must decide this the same way: the two
/// representations take different join and group-by paths, so the same data
/// arriving over two formats would otherwise perform differently for no
/// reason the user can see. The rule is deliberately relative — an absolute
/// cap is meaningless without knowing how many rows it is capping, and one
/// (4096, in the JSON reader) sat far below where the representation stops
/// paying: a 100k-value dictionary over 8M rows still beat plain strings by
/// 7x on a join, because the probe resolves each code once rather than
/// hashing a string per row.
///
/// The dictionary is indexed by `Column<Categorical>::code_type` (int32), so
/// this bound is a performance judgement, not an overflow guard.
[[nodiscard]] inline auto categorical_promotion_limit(std::size_t rows) noexcept -> std::size_t {
    return std::max<std::size_t>(
        1, static_cast<std::size_t>(static_cast<double>(rows) * kCategoricalPromotionRatio));
}

/// Specialization for categorical columns (dictionary-encoded strings).
template <>
class Column<Categorical> {
   public:
    /// What these values MEAN (e.g. the time zone of a Timestamp column), as
    /// opposed to where the rows sit. It travels with the column: sharing,
    /// copying, or gathering rows out of it all preserve it, because none of
    /// those change what a value means. See `ColumnMeta`.
    [[nodiscard]] auto meta() const noexcept -> const ColumnMeta& { return meta_; }
    void set_meta(ColumnMeta meta) noexcept { meta_ = meta; }
    using value_type = std::string_view;
    using size_type = std::size_t;
    using code_type = std::int32_t;
    using index_map =
        robin_hood::unordered_map<std::string, code_type, detail::StringHash, std::equal_to<>>;

    Column()
        : dict_(std::make_shared<std::vector<std::string>>()),
          index_(std::make_shared<index_map>()) {}

    explicit Column(std::vector<std::string> dict)
        : dict_(std::make_shared<std::vector<std::string>>(std::move(dict))),
          index_(std::make_shared<index_map>()) {
        rebuild_index();
    }

    Column(std::vector<std::string> dict, std::vector<code_type> codes)
        : dict_(std::make_shared<std::vector<std::string>>(std::move(dict))),
          index_(std::make_shared<index_map>()),
          codes_(std::move(codes)) {
        rebuild_index();
    }

    Column(std::shared_ptr<std::vector<std::string>> dict, std::shared_ptr<index_map> index,
           std::vector<code_type> codes = {})
        : dict_(std::move(dict)), index_(std::move(index)), codes_(std::move(codes)) {}

    Column(const Column& other)
        : dict_(other.dict_),
          index_(other.index_),
          codes_(other.codes_are_external() ? std::vector<code_type>{} : other.codes_),
          external_codes_owner_(other.external_codes_owner_),
          codes_data_(other.codes_are_external() ? other.codes_data_ : nullptr),
          codes_offset_(other.codes_are_external() ? other.codes_offset_ : 0),
          logical_size_(other.codes_are_external() ? other.logical_size_ : 0),
          external_dict_owner_(other.external_dict_owner_),
          dict_offsets_data_(other.dict_offsets_data_),
          dict_chars_data_(other.dict_chars_data_),
          dict_offset_(other.dict_offset_),
          dict_size_(other.dict_size_),
          dict_materialized_(other.dict_materialized_),
          meta_(other.meta_) {}

    Column(Column&& other) noexcept
        : dict_(std::move(other.dict_)),
          index_(std::move(other.index_)),
          codes_(std::move(other.codes_)),
          external_codes_owner_(std::move(other.external_codes_owner_)),
          codes_data_(external_codes_owner_ ? other.codes_data_ : nullptr),
          codes_offset_(external_codes_owner_ ? other.codes_offset_ : 0),
          logical_size_(external_codes_owner_ ? other.logical_size_ : 0),
          external_dict_owner_(std::move(other.external_dict_owner_)),
          dict_offsets_data_(other.dict_offsets_data_),
          dict_chars_data_(other.dict_chars_data_),
          dict_offset_(other.dict_offset_),
          dict_size_(other.dict_size_),
          dict_materialized_(other.dict_materialized_),
          meta_(other.meta_) {
        other.codes_data_ = nullptr;
        other.codes_offset_ = 0;
        other.logical_size_ = 0;
        other.dict_offsets_data_ = nullptr;
        other.dict_chars_data_ = nullptr;
        other.dict_offset_ = 0;
        other.dict_size_ = 0;
        other.dict_materialized_ = true;
    }

    auto operator=(const Column& other) -> Column& {
        if (this != &other) {
            Column copy(other);
            swap(copy);
        }
        return *this;
    }

    auto operator=(Column&& other) noexcept -> Column& {
        if (this != &other) {
            Column moved(std::move(other));
            swap(moved);
        }
        return *this;
    }

    /// Declared because the four copy/move members are: the rule of five is a
    /// readability rule here, not a correctness one — every member cleans up
    /// after itself, so the compiler-generated body is already right.
    ~Column() = default;

    void swap(Column& other) noexcept {
        dict_.swap(other.dict_);
        index_.swap(other.index_);
        codes_.swap(other.codes_);
        external_codes_owner_.swap(other.external_codes_owner_);
        std::swap(codes_data_, other.codes_data_);
        std::swap(codes_offset_, other.codes_offset_);
        std::swap(logical_size_, other.logical_size_);
        external_dict_owner_.swap(other.external_dict_owner_);
        std::swap(dict_offsets_data_, other.dict_offsets_data_);
        std::swap(dict_chars_data_, other.dict_chars_data_);
        std::swap(dict_offset_, other.dict_offset_);
        std::swap(dict_size_, other.dict_size_);
        std::swap(dict_materialized_, other.dict_materialized_);
        std::swap(meta_, other.meta_);
    }

    [[nodiscard]] static auto from_external(std::shared_ptr<const void> owner,
                                            const code_type* codes, size_type codes_offset,
                                            size_type size, const std::uint32_t* dict_offsets,
                                            const char* dict_chars, size_type dict_offset,
                                            size_type dict_size) -> Column {
        if (!owner) {
            throw std::invalid_argument("external categorical column requires a lifetime owner");
        }
        if ((codes == nullptr && size != 0) || dict_offsets == nullptr ||
            (dict_chars == nullptr && dict_offsets[dict_offset + dict_size] != 0)) {
            throw std::invalid_argument("external categorical column is missing a buffer");
        }
        Column column;
        column.dict_ = std::make_shared<std::vector<std::string>>();
        column.index_.reset();
        column.dict_materialized_ = false;
        column.external_codes_owner_ = owner;
        column.codes_.clear();
        column.codes_data_ = codes;
        column.codes_offset_ = codes_offset;
        column.logical_size_ = size;
        column.external_dict_owner_ = std::move(owner);
        column.dict_offsets_data_ = dict_offsets;
        column.dict_chars_data_ = dict_chars;
        column.dict_offset_ = dict_offset;
        column.dict_size_ = dict_size;
        return column;
    }

    [[nodiscard]] auto codes_are_external() const noexcept -> bool {
        return static_cast<bool>(external_codes_owner_);
    }
    [[nodiscard]] auto dictionary_is_external() const noexcept -> bool {
        return static_cast<bool>(external_dict_owner_);
    }
    [[nodiscard]] auto is_external() const noexcept -> bool {
        return codes_are_external() || dictionary_is_external();
    }
    [[nodiscard]] auto buffer_offset() const noexcept -> size_type {
        return codes_are_external() ? codes_offset_ : 0;
    }
    [[nodiscard]] auto codes_buffer_data() const noexcept -> const code_type* {
        return codes_are_external() ? codes_data_ : codes_.data();
    }
    [[nodiscard]] auto dictionary_buffer_offset() const noexcept -> size_type {
        return dict_offset_;
    }
    [[nodiscard]] auto dictionary_offsets_buffer_data() const noexcept -> const std::uint32_t* {
        return dict_offsets_data_;
    }
    [[nodiscard]] auto dictionary_chars_buffer_data() const noexcept -> const char* {
        return dict_chars_data_;
    }
    [[nodiscard]] auto dictionary_size() const noexcept -> size_type {
        return dictionary_is_external() ? dict_size_ : (dict_ == nullptr ? 0 : dict_->size());
    }

    /// Reads resolve the storage in use rather than a mirror kept in step by
    /// every mutation; see the same change on the primary template.
    [[nodiscard]] auto size() const noexcept -> size_type {
        return codes_are_external() ? logical_size_ : codes_.size();
    }
    [[nodiscard]] auto empty() const noexcept -> bool { return size() == 0; }

    [[nodiscard]] auto operator[](size_type idx) const noexcept -> value_type {
        if (dictionary_size() == 0) {
            return std::string_view{};
        }
        return dictionary_at(static_cast<size_type>(code_at(idx)));
    }

    [[nodiscard]] auto code_at(size_type idx) const noexcept -> code_type {
        return codes_are_external() ? codes_data_[codes_offset_ + idx] : codes_[idx];
    }

    void push_code(code_type code) {
        detach_codes();
        codes_.push_back(code);
    }

    /// Bulk-append already-resolved codes (e.g. from another Column<Categorical>
    /// proven to share this instance's dictionary). Codes are copied as-is
    /// with no dictionary lookup -- callers must ensure they are valid for
    /// this dictionary.
    template <typename InputIt>
    void append_codes(InputIt first, InputIt last) {
        detach_codes();
        codes_.insert(codes_.end(), first, last);
    }

    void push_back(value_type value) {
        detach_dictionary();
        detach_codes();
        auto code = find_or_insert(value);
        codes_.push_back(code);
    }

    void reserve(size_type capacity) {
        detach_codes();
        codes_.reserve(capacity);
    }
    void clear() noexcept {
        external_codes_owner_.reset();
        codes_.clear();
    }

    void resize(size_type count) {
        detach_codes();
        codes_.resize(count, 0);
    }

    [[nodiscard]] auto dictionary() const -> const std::vector<std::string>& {
        materialize_dictionary();
        return *dict_;
    }

    [[nodiscard]] auto dictionary_ptr() const -> const std::shared_ptr<std::vector<std::string>>& {
        materialize_dictionary();
        return dict_;
    }

    [[nodiscard]] auto index_ptr() const -> const std::shared_ptr<index_map>& {
        ensure_index();
        return index_;
    }

    [[nodiscard]] auto codes() const -> const std::vector<code_type>& {
        materialize_codes_cache();
        return codes_;
    }

    [[nodiscard]] auto codes_data() -> code_type* {
        detach_codes();
        return codes_.data();
    }
    [[nodiscard]] auto codes_data() const noexcept -> const code_type* {
        if (!codes_are_external()) {
            return codes_.data();
        }
        return codes_data_ == nullptr ? nullptr : codes_data_ + codes_offset_;
    }

    [[nodiscard]] auto find_code(value_type value) const -> std::optional<code_type> {
        ensure_index();
        auto it = index_->find(value);
        if (it == index_->end()) {
            return std::nullopt;
        }
        return it->second;
    }

   private:
    void rebuild_index() {
        dict_materialized_ = true;
        index_->clear();
        index_->reserve(dict_->size());
        for (std::size_t i = 0; i < dict_->size(); ++i) {
            index_->emplace((*dict_)[i], static_cast<code_type>(i));
        }
    }

    auto find_or_insert(value_type value) -> code_type {
        ensure_index();
        auto it = index_->find(value);
        if (it != index_->end()) {
            return it->second;
        }
        auto code = static_cast<code_type>(dict_->size());
        dict_->emplace_back(value);
        index_->emplace(dict_->back(), code);
        return code;
    }

    [[nodiscard]] auto dictionary_at(size_type idx) const noexcept -> value_type {
        if (!dictionary_is_external()) {
            return (*dict_)[idx];
        }
        const auto base = dict_offset_ + idx;
        const auto start = static_cast<size_type>(dict_offsets_data_[base]);
        const auto end = static_cast<size_type>(dict_offsets_data_[base + 1]);
        if (start == end) {
            return {};
        }
        return {dict_chars_data_ + start, end - start};
    }

    void materialize_dictionary() const {
        if (dict_materialized_) {
            return;
        }
        dict_->reserve(dict_size_);
        for (size_type i = 0; i < dict_size_; ++i) {
            dict_->emplace_back(dictionary_at(i));
        }
        dict_materialized_ = true;
    }

    void ensure_index() const {
        if (index_ != nullptr) {
            return;
        }
        index_ = std::make_shared<index_map>();
        index_->reserve(dictionary_size());
        for (size_type i = 0; i < dictionary_size(); ++i) {
            index_->emplace(std::string(dictionary_at(i)), static_cast<code_type>(i));
        }
    }

    void detach_dictionary() {
        materialize_dictionary();
        external_dict_owner_.reset();
        dict_offsets_data_ = nullptr;
        dict_chars_data_ = nullptr;
        dict_offset_ = 0;
        dict_size_ = dict_->size();
    }

    void materialize_codes_cache() const {
        if (!codes_are_external() || codes_.size() == logical_size_) {
            return;
        }
        if (logical_size_ == 0) {
            codes_.clear();
            return;
        }
        codes_.assign(codes_data(), codes_data() + logical_size_);
    }

    void detach_codes() {
        if (!codes_are_external()) {
            return;
        }
        materialize_codes_cache();
        external_codes_owner_.reset();
    }

    mutable std::shared_ptr<std::vector<std::string>> dict_;
    mutable std::shared_ptr<index_map> index_;
    mutable std::vector<code_type> codes_;
    std::shared_ptr<const void> external_codes_owner_;
    const code_type* codes_data_ = nullptr;
    size_type codes_offset_ = 0;
    size_type logical_size_ = 0;
    std::shared_ptr<const void> external_dict_owner_;
    const std::uint32_t* dict_offsets_data_ = nullptr;
    const char* dict_chars_data_ = nullptr;
    size_type dict_offset_ = 0;
    size_type dict_size_ = 0;
    mutable bool dict_materialized_ = true;

    /// See `meta()`. Declared last so the hand-written constructors can append
    /// it to their member-init lists without tripping -Wreorder. Four bytes,
    /// and trivially copyable -- deliberately, since a column is copied per
    /// chunk in the chunked paths.
    ColumnMeta meta_;
};

/// Specialization for non-categorical strings using an Arrow-style flat buffer.
///
/// Storage layout:
///   offsets_: n+1 uint32_t values; offsets_[i..i+1) is the char range of row i
///   chars_:   all string bytes concatenated contiguously
///
/// Benefits over vector<string>:
///   - No per-string heap allocation regardless of length
///   - Filter gather is 2-pass memcpy (sequential reads on sorted indices)
///   - Zero SSO overhead for large-cardinality string columns
template <>
class Column<std::string> {
    // NoInitAllocator (as Column<T> uses) so that resizing to make room for a
    // bulk write does not first zero-fill the buffer. On a 6M-row string column
    // the character buffer is ~160MB, and value-initializing it costs more than
    // the bulk append saves — see begin_bulk_append and resize_for_gather, both
    // of which overwrite every byte they expose.
    std::vector<std::uint32_t, detail::NoInitAllocator<std::uint32_t>>
        offsets_;                                             // size = rows+1; offsets_[0]=0 always
    std::vector<char, detail::NoInitAllocator<char>> chars_;  // all string bytes concatenated
    std::shared_ptr<const void> external_owner_;
    const std::uint32_t* offsets_data_ = nullptr;
    const char* chars_data_ = nullptr;
    std::size_t external_offset_ = 0;
    std::size_t logical_size_ = 0;

   public:
    /// What these values MEAN (e.g. the time zone of a Timestamp column), as
    /// opposed to where the rows sit. It travels with the column: sharing,
    /// copying, or gathering rows out of it all preserve it, because none of
    /// those change what a value means. See `ColumnMeta`.
    [[nodiscard]] auto meta() const noexcept -> const ColumnMeta& { return meta_; }
    void set_meta(ColumnMeta meta) noexcept { meta_ = meta; }
    using value_type = std::string_view;
    using size_type = std::size_t;

    // Default: empty column ready to receive push_backs.
    Column() { offsets_.push_back(0); }

    // From vector<string> (used by CSV reader).
    explicit Column(const std::vector<std::string>& vals) {
        offsets_.reserve(vals.size() + 1);
        offsets_.push_back(0);
        std::size_t total = 0;
        for (const auto& s : vals)
            total += s.size();
        chars_.reserve(total);
        for (const auto& s : vals) {
            chars_.insert(chars_.end(), s.begin(), s.end());
            offsets_.push_back(static_cast<std::uint32_t>(chars_.size()));
        }
    }

    // Initializer list (used in REPL, tests); const char* → string_view is implicit.
    Column(std::initializer_list<std::string_view> init) : Column() {
        for (auto sv : init)
            push_back(sv);
    }

    Column(const Column& other)
        : offsets_(other.is_external() ? decltype(offsets_){} : other.offsets_),
          chars_(other.is_external() ? decltype(chars_){} : other.chars_),
          external_owner_(other.external_owner_),
          offsets_data_(other.is_external() ? other.offsets_data_ : nullptr),
          chars_data_(other.is_external() ? other.chars_data_ : nullptr),
          external_offset_(other.is_external() ? other.external_offset_ : 0),
          logical_size_(other.is_external() ? other.logical_size_ : 0),
          meta_(other.meta_) {}

    Column(Column&& other) noexcept
        : offsets_(std::move(other.offsets_)),
          chars_(std::move(other.chars_)),
          external_owner_(std::move(other.external_owner_)),
          offsets_data_(external_owner_ ? other.offsets_data_ : nullptr),
          chars_data_(external_owner_ ? other.chars_data_ : nullptr),
          external_offset_(external_owner_ ? other.external_offset_ : 0),
          logical_size_(external_owner_ ? other.logical_size_ : 0),
          meta_(other.meta_) {
        other.offsets_data_ = nullptr;
        other.chars_data_ = nullptr;
        other.external_offset_ = 0;
        other.logical_size_ = 0;
    }

    auto operator=(const Column& other) -> Column& {
        if (this != &other) {
            Column copy(other);
            swap(copy);
        }
        return *this;
    }

    auto operator=(Column&& other) noexcept -> Column& {
        if (this != &other) {
            Column moved(std::move(other));
            swap(moved);
        }
        return *this;
    }

    /// Declared because the four copy/move members are: the rule of five is a
    /// readability rule here, not a correctness one — every member cleans up
    /// after itself, so the compiler-generated body is already right.
    ~Column() = default;

    void swap(Column& other) noexcept {
        offsets_.swap(other.offsets_);
        chars_.swap(other.chars_);
        external_owner_.swap(other.external_owner_);
        std::swap(offsets_data_, other.offsets_data_);
        std::swap(chars_data_, other.chars_data_);
        std::swap(external_offset_, other.external_offset_);
        std::swap(logical_size_, other.logical_size_);
        std::swap(meta_, other.meta_);
    }

    [[nodiscard]] static auto from_external(std::shared_ptr<const void> owner,
                                            const std::uint32_t* offsets, const char* chars,
                                            size_type offset, size_type size) -> Column {
        if (!owner) {
            throw std::invalid_argument("external string column requires a lifetime owner");
        }
        if (offsets == nullptr) {
            throw std::invalid_argument("external string column requires offsets");
        }
        if (chars == nullptr && offsets[offset + size] != 0) {
            throw std::invalid_argument("external string column requires a character buffer");
        }
        Column column;
        column.offsets_.clear();
        column.external_owner_ = std::move(owner);
        column.offsets_data_ = offsets;
        column.chars_data_ = chars;
        column.external_offset_ = offset;
        column.logical_size_ = size;
        return column;
    }

    [[nodiscard]] auto is_external() const noexcept -> bool {
        return static_cast<bool>(external_owner_);
    }
    [[nodiscard]] auto buffer_offset() const noexcept -> size_type {
        return is_external() ? external_offset_ : 0;
    }
    [[nodiscard]] auto offsets_buffer_data() const noexcept -> const std::uint32_t* {
        return is_external() ? offsets_data_ : offsets_.data();
    }
    [[nodiscard]] auto chars_buffer_data() const noexcept -> const char* {
        return is_external() ? chars_data_ : chars_.data();
    }

    /// Reads resolve the storage in use rather than a mirror kept in step by
    /// every mutation; see the same change on the primary template. Appending a
    /// string wrote four members per call for the benefit of these reads.
    [[nodiscard]] auto size() const noexcept -> size_type {
        if (is_external()) {
            return logical_size_;
        }
        return offsets_.empty() ? 0 : offsets_.size() - 1;
    }
    [[nodiscard]] auto empty() const noexcept -> bool { return size() == 0; }

    [[nodiscard]] auto operator[](size_type i) const noexcept -> std::string_view {
        const auto* offsets = offsets_data();
        const auto start = static_cast<size_type>(offsets[i]);
        const auto end = static_cast<size_type>(offsets[i + 1]);
        if (start == end) {
            return {};
        }
        return {chars_data() + start, end - start};
    }

    [[nodiscard]] auto at(size_type i) const -> std::string_view {
        if (i >= size())
            throw std::out_of_range("Column<std::string>::at");
        return (*this)[i];
    }

    void push_back(std::string_view sv) {
        detach_external();
        chars_.insert(chars_.end(), sv.begin(), sv.end());
        offsets_.push_back(static_cast<std::uint32_t>(chars_.size()));
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    void reserve(size_type n, size_type chars_hint = 0) {
        detach_external();
        offsets_.reserve(n + 1);
        if (chars_hint)
            chars_.reserve(chars_hint);
    }

    /// Bulk append through raw cursors, for producers that know an upper bound
    /// on the bytes they are about to write (the Parquet decoder gets one from
    /// the column chunk metadata).
    ///
    /// `push_back` costs a `vector::insert` plus an `offsets_` push per value —
    /// a capacity check, a memmove call, and a size update each time. On a
    /// 6M-row string column that is as expensive as the Parquet decode feeding
    /// it: 112ms against 55ms for the same bytes written through cursors. The
    /// storage is sized once, then written straight through.
    ///
    /// Usage: `auto w = begin_bulk_append(rows, chars_upper_bound);` then
    /// `w.append(sv)` exactly `rows` times, then `finish_bulk_append(w)`, which
    /// trims the character buffer to what was actually written. Writing more
    /// than `rows` values or more than `chars_upper_bound` bytes is undefined.
    class BulkAppender {
        friend class Column<std::string>;
        char* chars_ = nullptr;             // write cursor
        std::uint32_t* offsets_ = nullptr;  // write cursor
        const char* chars_begin_ = nullptr;

       public:
        void append(std::string_view value) noexcept {
            if (!value.empty()) {
                std::memcpy(chars_, value.data(), value.size());
                chars_ += value.size();
            }
            *offsets_++ = static_cast<std::uint32_t>(chars_ - chars_begin_);
        }
    };

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto begin_bulk_append(size_type rows, size_type chars_upper_bound)
        -> BulkAppender {
        detach_external();
        const size_type old_rows = size();
        const size_type old_chars = chars_.size();
        offsets_.resize(old_rows + rows + 1);
        chars_.resize(old_chars + chars_upper_bound);

        BulkAppender writer;
        writer.chars_begin_ = chars_.data();
        writer.chars_ = chars_.data() + old_chars;
        writer.offsets_ = offsets_.data() + old_rows + 1;
        return writer;
    }

    void finish_bulk_append(const BulkAppender& writer) {
        const auto written = static_cast<size_type>(writer.chars_ - writer.chars_begin_);
        chars_.resize(written);
        offsets_.resize(static_cast<size_type>(writer.offsets_ - offsets_.data()));
    }

    void clear() noexcept {
        drop_external();
        offsets_.clear();
        offsets_.push_back(0);
        chars_.clear();
    }

    // Raw access for optimized gather in filter_table.
    [[nodiscard]] const std::uint32_t* offsets_data() const noexcept {
        return is_external() ? offsets_data_ + external_offset_ : offsets_.data();
    }
    [[nodiscard]] const char* chars_data() const noexcept {
        return is_external() ? chars_data_ : chars_.data();
    }
    [[nodiscard]] std::uint32_t* offsets_data() {
        detach_external();
        return offsets_.data();
    }
    [[nodiscard]] char* chars_data() {
        detach_external();
        return chars_.data();
    }

    // Resize to n rows, all filled with the same value.
    void resize(size_type n, std::string_view fill = {}) {
        drop_external();
        offsets_.clear();
        chars_.clear();
        offsets_.reserve(n + 1);
        offsets_.push_back(0);
        if (n > 0 && !fill.empty())
            chars_.reserve(n * fill.size());
        for (size_type i = 0; i < n; ++i) {
            chars_.insert(chars_.end(), fill.begin(), fill.end());
            offsets_.push_back(static_cast<std::uint32_t>(chars_.size()));
        }
    }

    // Allocate output storage for a gather of n_rows rows with total_chars bytes.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    void resize_for_gather(size_type n_rows, size_type total_chars) {
        drop_external();
        offsets_.resize(n_rows + 1);
        chars_.resize(total_chars);
    }

    // Iterator: yields string_view per row.
    struct Iterator {
        const Column* col;
        size_type i;
        auto operator*() const -> std::string_view { return (*col)[i]; }
        auto operator++() -> Iterator& {
            ++i;
            return *this;
        }
        auto operator==(const Iterator& o) const -> bool { return i == o.i; }
        auto operator!=(const Iterator& o) const -> bool { return i != o.i; }
    };
    [[nodiscard]] auto begin() const noexcept -> Iterator { return {this, 0}; }
    [[nodiscard]] auto end() const noexcept -> Iterator { return {this, size()}; }

   private:
    void drop_external() noexcept {
        external_owner_.reset();
        external_offset_ = 0;
    }

    void detach_external() {
        if (!is_external()) {
            return;
        }
        const auto* source_offsets = offsets_data_ + external_offset_;
        const auto first_char = static_cast<size_type>(source_offsets[0]);
        const auto last_char = static_cast<size_type>(source_offsets[logical_size_]);

        decltype(offsets_) owned_offsets(logical_size_ + 1);
        for (size_type i = 0; i <= logical_size_; ++i) {
            owned_offsets[i] =
                static_cast<std::uint32_t>(static_cast<size_type>(source_offsets[i]) - first_char);
        }
        decltype(chars_) owned_chars;
        if (last_char != first_char) {
            owned_chars.insert(owned_chars.end(), chars_data_ + first_char,
                               chars_data_ + last_char);
        }
        offsets_ = std::move(owned_offsets);
        chars_ = std::move(owned_chars);
        drop_external();
    }

    /// See `meta()`. Declared last so the hand-written constructors can append
    /// it to their member-init lists without tripping -Wreorder. Four bytes,
    /// and trivially copyable -- deliberately, since a column is copied per
    /// chunk in the chunked paths.
    ColumnMeta meta_;
};

/// Explicit specialisation for bool.
///
/// `std::vector<bool>` is a space-optimised bitset with awkward proxy semantics
/// and no stable raw data pointer. We keep an explicit packed bitset instead so
/// bool columns use 1 bit per row while preserving a predictable value API.
template <>
class Column<bool> {
   public:
    /// What these values MEAN (e.g. the time zone of a Timestamp column), as
    /// opposed to where the rows sit. It travels with the column: sharing,
    /// copying, or gathering rows out of it all preserve it, because none of
    /// those change what a value means. See `ColumnMeta`.
    [[nodiscard]] auto meta() const noexcept -> const ColumnMeta& { return meta_; }
    void set_meta(ColumnMeta meta) noexcept { meta_ = meta; }
    using value_type = bool;
    using size_type = std::size_t;
    using word_type = std::uint64_t;

   private:
    static constexpr size_type kBitsPerWord = sizeof(word_type) * 8;

    static constexpr auto word_index(size_type bit) noexcept -> size_type {
        return bit / kBitsPerWord;
    }
    static constexpr auto bit_offset(size_type bit) noexcept -> size_type {
        return bit % kBitsPerWord;
    }
    static constexpr auto bit_mask(size_type bit) noexcept -> word_type {
        return word_type{1} << bit_offset(bit);
    }
    static constexpr auto words_for_bits(size_type bits) noexcept -> size_type {
        return (bits + kBitsPerWord - 1) / kBitsPerWord;
    }
    static constexpr auto low_bits_mask(size_type bits) noexcept -> word_type {
        if (bits == 0) {
            return word_type{0};
        }
        if (bits >= kBitsPerWord) {
            return ~word_type{0};
        }
        return (word_type{1} << bits) - 1;
    }

    std::vector<word_type> words_;
    std::shared_ptr<const void> external_owner_;
    const std::uint8_t* bytes_data_ = nullptr;
    size_type external_offset_ = 0;
    size_type size_bits_ = 0;

    auto clear_unused_tail_bits() noexcept -> void {
        if (words_.empty()) {
            return;
        }
        const size_type rem = bit_offset(size_bits_);
        if (rem == 0) {
            return;
        }
        words_.back() &= low_bits_mask(rem);
    }

   public:
    class Reference {
        word_type* word_ = nullptr;
        word_type mask_ = 0;

       public:
        explicit Reference(word_type* word, word_type mask) : word_(word), mask_(mask) {}
        Reference(const Reference&) = default;
        Reference(Reference&&) = default;
        ~Reference() = default;

        auto operator=(bool v) -> Reference& {
            if (v) {
                *word_ |= mask_;
            } else {
                *word_ &= ~mask_;
            }
            return *this;
        }

        // NOLINTNEXTLINE(cert-oop54-cpp, bugprone-unhandled-self-assignment)
        auto operator=(const Reference& other) -> Reference& {
            return *this = static_cast<bool>(other);
        }

        auto operator=(Reference&& other) noexcept -> Reference& {
            return *this = static_cast<bool>(other);
        }

        [[nodiscard]] operator bool() const { return (*word_ & mask_) != 0; }
    };

    Column() = default;

    explicit Column(size_type count, bool value = false) { assign(count, value); }

    Column(const std::vector<bool>& bools) {
        reserve(bools.size());
        for (const bool v : bools)
            push_back(v);
    }

    Column(std::initializer_list<bool> init) {
        reserve(init.size());
        for (const bool v : init)
            push_back(v);
    }

    Column(const Column& other)
        : words_(other.is_external() ? std::vector<word_type>{} : other.words_),
          external_owner_(other.external_owner_),
          bytes_data_(other.is_external() ? other.bytes_data_ : nullptr),
          external_offset_(other.is_external() ? other.external_offset_ : 0),
          size_bits_(other.size_bits_),
          meta_(other.meta_) {}

    Column(Column&& other) noexcept
        : words_(std::move(other.words_)),
          external_owner_(std::move(other.external_owner_)),
          bytes_data_(external_owner_ ? other.bytes_data_ : nullptr),
          external_offset_(external_owner_ ? other.external_offset_ : 0),
          size_bits_(other.size_bits_),
          meta_(other.meta_) {
        other.bytes_data_ = nullptr;
        other.external_offset_ = 0;
        other.size_bits_ = 0;
    }

    auto operator=(const Column& other) -> Column& {
        if (this != &other) {
            Column copy(other);
            swap(copy);
        }
        return *this;
    }

    auto operator=(Column&& other) noexcept -> Column& {
        if (this != &other) {
            Column moved(std::move(other));
            swap(moved);
        }
        return *this;
    }

    /// Declared because the four copy/move members are: the rule of five is a
    /// readability rule here, not a correctness one — every member cleans up
    /// after itself, so the compiler-generated body is already right.
    ~Column() = default;

    void swap(Column& other) noexcept {
        words_.swap(other.words_);
        external_owner_.swap(other.external_owner_);
        std::swap(bytes_data_, other.bytes_data_);
        std::swap(external_offset_, other.external_offset_);
        std::swap(size_bits_, other.size_bits_);
        std::swap(meta_, other.meta_);
    }

    [[nodiscard]] static auto from_external(std::shared_ptr<const void> owner,
                                            const std::uint8_t* bytes, size_type offset,
                                            size_type size) -> Column {
        if (!owner) {
            throw std::invalid_argument("external bool column requires a lifetime owner");
        }
        if (bytes == nullptr && size != 0) {
            throw std::invalid_argument("external bool column requires a bitmap buffer");
        }
        Column column;
        column.external_owner_ = std::move(owner);
        column.bytes_data_ = bytes;
        column.external_offset_ = offset;
        column.size_bits_ = size;
        return column;
    }

    [[nodiscard]] auto is_external() const noexcept -> bool {
        return static_cast<bool>(external_owner_);
    }
    /// Reads resolve the storage in use rather than a mirror kept in step by
    /// every mutation; see the same change on the primary template. `size_bits_`
    /// is genuine state in both modes and stays a plain member.
    [[nodiscard]] auto buffer_data() const noexcept -> const std::uint8_t* {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return is_external() ? bytes_data_ : reinterpret_cast<const std::uint8_t*>(words_.data());
    }
    [[nodiscard]] auto buffer_offset() const noexcept -> size_type {
        return is_external() ? external_offset_ : 0;
    }

    [[nodiscard]] auto size() const noexcept -> size_type { return size_bits_; }
    [[nodiscard]] auto empty() const noexcept -> bool { return size_bits_ == 0; }
    [[nodiscard]] auto word_count() const noexcept -> size_type {
        return words_for_bits(size_bits_);
    }

    [[nodiscard]] auto operator[](size_type idx) const noexcept -> bool {
        const size_type bit = buffer_offset() + idx;
        return ((buffer_data()[bit / 8] >> (bit % 8)) & 0x01U) != 0U;
    }
    [[nodiscard]] auto operator[](size_type idx) -> Reference {
        detach_external();
        return Reference(&words_[word_index(idx)], bit_mask(idx));
    }

    auto operator=(const std::vector<bool>& bools) -> Column& {
        clear();
        reserve(bools.size());
        for (const bool v : bools)
            push_back(v);
        return *this;
    }

    auto set(size_type idx, bool value) -> void {
        detach_external();
        auto& word = words_[word_index(idx)];
        const word_type mask = bit_mask(idx);
        if (value) {
            word |= mask;
        } else {
            word &= ~mask;
        }
    }

    void push_back(bool value) {
        detach_external();
        const size_type idx = size_bits_;
        if (bit_offset(idx) == 0) {
            words_.push_back(0);
        }
        if (value) {
            words_.back() |= bit_mask(idx);
        }
        ++size_bits_;
    }

    void reserve(size_type n) {
        detach_external();
        words_.reserve(words_for_bits(n));
    }

    // zero-initialises (false) for resize-based fill in lag/lead paths
    void resize(size_type n) { resize(n, false); }
    void resize(size_type n, bool value) {
        detach_external();
        const size_type old_size = size_bits_;
        if (n == old_size) {
            return;
        }
        words_.resize(words_for_bits(n), 0);
        size_bits_ = n;
        if (n > old_size && value) {
            for (size_type i = old_size; i < n; ++i) {
                set(i, true);
            }
        }
        clear_unused_tail_bits();
    }

    [[nodiscard]] auto words_data() const noexcept -> const word_type* {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        return is_external() ? reinterpret_cast<const word_type*>(bytes_data_) : words_.data();
    }
    [[nodiscard]] auto words_data() -> word_type* {
        detach_external();
        return words_.data();
    }

    void clear() noexcept {
        drop_external();
        words_.clear();
        size_bits_ = 0;
    }

    void assign(size_type count, bool value) {
        drop_external();
        words_.assign(words_for_bits(count), value ? ~word_type{0} : word_type{0});
        size_bits_ = count;
        clear_unused_tail_bits();
    }

    struct Iterator {
        const Column* col = nullptr;
        size_type i = 0;
        auto operator*() const -> bool { return (*col)[i]; }
        auto operator++() -> Iterator& {
            ++i;
            return *this;
        }
        auto operator==(const Iterator& other) const -> bool { return i == other.i; }
        auto operator!=(const Iterator& other) const -> bool { return i != other.i; }
    };

    [[nodiscard]] auto begin() const noexcept -> Iterator { return {this, 0}; }
    [[nodiscard]] auto end() const noexcept -> Iterator { return {this, size()}; }

   private:
    void drop_external() noexcept {
        external_owner_.reset();
        external_offset_ = 0;
    }

    void detach_external() {
        if (!is_external()) {
            return;
        }
        const auto* source = bytes_data_;
        const auto source_offset = external_offset_;
        std::vector<word_type> owned(words_for_bits(size_bits_), 0);
        for (size_type i = 0; i < size_bits_; ++i) {
            const size_type bit = source_offset + i;
            if (((source[bit / 8] >> (bit % 8)) & 0x01U) != 0U) {
                owned[word_index(i)] |= bit_mask(i);
            }
        }
        words_ = std::move(owned);
        drop_external();
    }

    /// See `meta()`. Declared last so the hand-written constructors can append
    /// it to their member-init lists without tripping -Wreorder. Four bytes,
    /// and trivially copyable -- deliberately, since a column is copied per
    /// chunk in the chunked paths.
    ColumnMeta meta_;
};

/// Whether `ColT` stores elements as a dense array of `value_type`, so a kernel
/// can size it once and write through `data()`.
///
/// The three specializations are all false: `Column<bool>` bit-packs,
/// `Column<Categorical>` stores codes against a dictionary, and
/// `Column<std::string>` a flat character buffer with offsets — in none of them
/// is `value_type` what is actually stored. `ColumnAppender` is the intended
/// consumer; see it for why the distinction is worth making.
template <typename ColT>
inline constexpr bool is_dense_column_v = false;
template <typename T>
inline constexpr bool is_dense_column_v<Column<T>> = true;
template <>
inline constexpr bool is_dense_column_v<Column<bool>> = false;
template <>
inline constexpr bool is_dense_column_v<Column<Categorical>> = false;
template <>
inline constexpr bool is_dense_column_v<Column<std::string>> = false;

/// Fills a column of known length front-to-back, resolving its storage once.
///
/// A `Column` may hold adopted (Arrow) storage, so `push_back` tests which
/// storage is in use on every call. That check costs more than the append
/// itself in a per-row kernel, and it cannot be hoisted by the compiler because
/// the branch it guards may change the state it tests. Resolving it here, once,
/// is what a hand-hoisted `data()` pointer does — but this also works in a
/// kernel generic over every column type, which is why the fill and rolling
/// kernels could not simply hoist.
///
/// The row count must be exact: dense columns are sized up front, so pushing
/// more than `rows` writes out of bounds. Kernels that emit one value per input
/// row (which is all of them here) satisfy that by construction.
template <typename ColT>
class ColumnAppender {
   public:
    using value_type = ColT::value_type;

    ColumnAppender(ColT& column, std::size_t rows) : column_(&column) {
        if constexpr (is_dense_column_v<ColT>) {
            column_->resize(rows);
            out_ = column_->data();
        } else {
            column_->reserve(rows);
        }
    }

    void push(const value_type& value) {
        if constexpr (is_dense_column_v<ColT>) {
            out_[next_++] = value;
        } else {
            column_->push_back(value);
        }
    }

   private:
    ColT* column_;
    value_type* out_ = nullptr;
    std::size_t next_ = 0;
};

}  // namespace ibex
