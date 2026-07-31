#pragma once

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
    constexpr NoInitAllocator(const NoInitAllocator<U>&) noexcept {}

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
auto operator==(const NoInitAllocator<T>&, const NoInitAllocator<U>&) noexcept -> bool {
    return true;
}

template <typename T, typename U>
auto operator!=(const NoInitAllocator<T>&, const NoInitAllocator<U>&) noexcept -> bool {
    return false;
}
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
        : data_(std::make_move_iterator(data.begin()), std::make_move_iterator(data.end())) {
        sync_owned_view();
    }

    Column(std::initializer_list<T> init) : data_(init) { sync_owned_view(); }

    Column(const Column& other)
        : data_(other.is_external() ? storage_type{} : other.data_),
          external_owner_(other.external_owner_),
          external_data_(other.is_external() ? other.external_data_ : data_.data()),
          external_offset_(other.external_offset_),
          external_size_(other.external_size_) {}

    Column(Column&& other) noexcept
        : data_(std::move(other.data_)),
          external_owner_(std::move(other.external_owner_)),
          external_data_(external_owner_ ? other.external_data_ : data_.data()),
          external_offset_(other.external_offset_),
          external_size_(other.external_size_) {
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

    void swap(Column& other) noexcept {
        data_.swap(other.data_);
        external_owner_.swap(other.external_owner_);
        std::swap(external_data_, other.external_data_);
        std::swap(external_offset_, other.external_offset_);
        std::swap(external_size_, other.external_size_);
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
        if (values == nullptr) {
            throw std::invalid_argument("external column requires a data buffer");
        }
        Column column;
        column.external_owner_ = std::move(owner);
        column.external_data_ = values + offset;
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
        return is_external() ? external_data_ - external_offset_ : external_data_;
    }
    [[nodiscard]] auto buffer_offset() const noexcept -> size_type {
        return is_external() ? external_offset_ : 0;
    }

    /// Number of elements.
    [[nodiscard]] auto size() const noexcept -> size_type { return external_size_; }

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
    void push_back(const T& value) {
        detach_external();
        data_.push_back(value);
        sync_owned_view();
    }
    void push_back(T&& value) {
        detach_external();
        data_.push_back(std::move(value));
        sync_owned_view();
    }

    /// Construct a value in-place.
    template <typename... Args>
        requires std::constructible_from<T, Args...>
    auto emplace_back(Args&&... args) -> T& {
        detach_external();
        data_.emplace_back(std::forward<Args>(args)...);
        sync_owned_view();
        return data_.back();
    }

    auto emplace_back() -> T& {
        detach_external();
        data_.emplace_back(T{});
        sync_owned_view();
        return data_.back();
    }

    /// Assign from count and value.
    void assign(size_type count, const T& value) {
        drop_external();
        data_.assign(count, value);
        sync_owned_view();
    }

    /// Assign from range.
    template <typename InputIt>
        requires std::input_iterator<InputIt>
    void assign(InputIt first, InputIt last) {
        storage_type assigned(first, last);
        drop_external();
        data_ = std::move(assigned);
        sync_owned_view();
    }
    /// Assign from initializer list.
    void assign(std::initializer_list<T> init) {
        drop_external();
        data_.assign(init);
        sync_owned_view();
    }

    /// Insert value before position.
    [[nodiscard]] auto insert(const_iterator pos, const T& value) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), value);
        sync_owned_view();
        return iterator_at(offset);
    }
    [[nodiscard]] auto insert(const_iterator pos, T&& value) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), std::move(value));
        sync_owned_view();
        return iterator_at(offset);
    }

    /// Insert count copies of value.
    [[nodiscard]] auto insert(const_iterator pos, size_type count, const T& value) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), count, value);
        sync_owned_view();
        return iterator_at(offset);
    }

    /// Insert range.
    template <typename InputIt>
        requires std::input_iterator<InputIt>
    [[nodiscard]] auto insert(const_iterator pos, InputIt first, InputIt last) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.insert(data_.begin() + static_cast<std::ptrdiff_t>(offset), first, last);
        sync_owned_view();
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
        sync_owned_view();
        return iterator_at(offset);
    }

    /// Erase element at position.
    [[nodiscard]] auto erase(const_iterator pos) -> iterator {
        const auto offset = iterator_offset(pos);
        detach_external();
        data_.erase(data_.begin() + static_cast<std::ptrdiff_t>(offset));
        const auto next = std::min(offset, data_.size());
        sync_owned_view();
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
        sync_owned_view();
        return iterator_at(next);
    }

    /// Reserve capacity.
    void reserve(size_type capacity) {
        detach_external();
        data_.reserve(capacity);
        sync_owned_view();
    }

    /// Current capacity.
    [[nodiscard]] auto capacity() const noexcept -> size_type {
        return is_external() ? external_size_ : data_.capacity();
    }

    /// Remove all elements.
    void clear() noexcept {
        drop_external();
        data_.clear();
        sync_owned_view();
    }

    /// Resize the column (value-initialize new elements).
    void resize(size_type count) {
        detach_external();
        data_.resize(count, T{});
        sync_owned_view();
    }

    /// Resize the column (copy-initialize new elements with value).
    void resize(size_type count, const T& value) {
        detach_external();
        data_.resize(count, value);
        sync_owned_view();
    }

    /// Resize without value-initializing new slots. Callers must overwrite every element.
    void resize_for_overwrite(size_type count)
        requires std::is_trivially_default_constructible_v<T>
    {
        // Existing values must survive `resize` when count is smaller or when
        // the caller grows relative to an adopted slice.
        detach_external();
        data_.resize(count);
        sync_owned_view();
    }

    /// Reduce capacity to fit size.
    void shrink_to_fit() {
        detach_external();
        data_.shrink_to_fit();
        sync_owned_view();
    }

    /// Remove the last element.
    void pop_back() {
        detach_external();
        data_.pop_back();
        sync_owned_view();
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
        return iterator_at(external_size_);
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
    [[nodiscard]] auto data_ptr() const noexcept -> const T* { return external_data_; }

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

    void detach_external() {
        if (!is_external()) {
            return;
        }
        storage_type owned;
        owned.reserve(external_size_);
        if (external_size_ != 0) {
            owned.insert(owned.end(), external_data_, external_data_ + external_size_);
        }
        data_ = std::move(owned);
        drop_external();
        sync_owned_view();
    }

    void sync_owned_view() noexcept {
        external_data_ = data_.data();
        external_size_ = data_.size();
        external_offset_ = 0;
    }

    [[nodiscard]] auto iterator_at(size_type offset) noexcept -> iterator {
        return external_size_ == 0 ? data_.data() : data_.data() + offset;
    }

    storage_type data_;
    std::shared_ptr<const void> external_owner_;
    const T* external_data_ = nullptr;
    size_type external_offset_ = 0;
    size_type external_size_ = 0;
};

/// Specialization for categorical columns (dictionary-encoded strings).
template <>
class Column<Categorical> {
   public:
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

    [[nodiscard]] auto size() const noexcept -> size_type { return codes_.size(); }
    [[nodiscard]] auto empty() const noexcept -> bool { return codes_.empty(); }

    [[nodiscard]] auto operator[](size_type idx) const noexcept -> value_type {
        if (dict_ == nullptr || dict_->empty()) {
            return std::string_view{};
        }
        return (*dict_)[static_cast<std::size_t>(codes_[idx])];
    }

    [[nodiscard]] auto code_at(size_type idx) const noexcept -> code_type { return codes_[idx]; }

    void push_code(code_type code) { codes_.push_back(code); }

    /// Bulk-append already-resolved codes (e.g. from another Column<Categorical>
    /// proven to share this instance's dictionary). Codes are copied as-is
    /// with no dictionary lookup -- callers must ensure they are valid for
    /// this dictionary.
    template <typename InputIt>
    void append_codes(InputIt first, InputIt last) {
        codes_.insert(codes_.end(), first, last);
    }

    void push_back(value_type value) {
        auto code = find_or_insert(value);
        codes_.push_back(code);
    }

    void reserve(size_type capacity) { codes_.reserve(capacity); }
    void clear() noexcept { codes_.clear(); }

    void resize(size_type count) { codes_.resize(count, 0); }

    [[nodiscard]] auto dictionary() const noexcept -> const std::vector<std::string>& {
        return *dict_;
    }

    [[nodiscard]] auto dictionary_ptr() const noexcept
        -> const std::shared_ptr<std::vector<std::string>>& {
        return dict_;
    }

    [[nodiscard]] auto index_ptr() const noexcept -> const std::shared_ptr<index_map>& {
        return index_;
    }

    [[nodiscard]] auto codes() const noexcept -> const std::vector<code_type>& { return codes_; }

    [[nodiscard]] auto codes_data() noexcept -> code_type* { return codes_.data(); }
    [[nodiscard]] auto codes_data() const noexcept -> const code_type* { return codes_.data(); }

    [[nodiscard]] auto find_code(value_type value) const -> std::optional<code_type> {
        if (index_ == nullptr) {
            return std::nullopt;
        }
        auto it = index_->find(value);
        if (it == index_->end()) {
            return std::nullopt;
        }
        return it->second;
    }

   private:
    void rebuild_index() {
        index_->clear();
        index_->reserve(dict_->size());
        for (std::size_t i = 0; i < dict_->size(); ++i) {
            index_->emplace((*dict_)[i], static_cast<code_type>(i));
        }
    }

    auto find_or_insert(value_type value) -> code_type {
        if (index_ == nullptr) {
            index_ = std::make_shared<index_map>();
        }
        auto it = index_->find(value);
        if (it != index_->end()) {
            return it->second;
        }
        auto code = static_cast<code_type>(dict_->size());
        dict_->emplace_back(value);
        index_->emplace(dict_->back(), code);
        return code;
    }

    std::shared_ptr<std::vector<std::string>> dict_;
    std::shared_ptr<index_map> index_;
    std::vector<code_type> codes_;
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

   public:
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

    [[nodiscard]] auto size() const noexcept -> size_type { return offsets_.size() - 1; }
    [[nodiscard]] auto empty() const noexcept -> bool { return offsets_.size() == 1; }

    [[nodiscard]] auto operator[](size_type i) const noexcept -> std::string_view {
        const auto start = static_cast<size_type>(offsets_[i]);
        const auto end = static_cast<size_type>(offsets_[i + 1]);

        auto slice = std::span<const char>(chars_).subspan(start, end - start);
        return {slice.data(), slice.size()};
    }

    [[nodiscard]] auto at(size_type i) const -> std::string_view {
        if (i >= size())
            throw std::out_of_range("Column<std::string>::at");
        return (*this)[i];
    }

    void push_back(std::string_view sv) {
        chars_.insert(chars_.end(), sv.begin(), sv.end());
        offsets_.push_back(static_cast<std::uint32_t>(chars_.size()));
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    void reserve(size_type n, size_type chars_hint = 0) {
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
        offsets_.clear();
        offsets_.push_back(0);
        chars_.clear();
    }

    // Raw access for optimized gather in filter_table.
    [[nodiscard]] const std::uint32_t* offsets_data() const noexcept { return offsets_.data(); }
    [[nodiscard]] const char* chars_data() const noexcept { return chars_.data(); }
    [[nodiscard]] std::uint32_t* offsets_data() noexcept { return offsets_.data(); }
    [[nodiscard]] char* chars_data() noexcept { return chars_.data(); }

    // Resize to n rows, all filled with the same value.
    void resize(size_type n, std::string_view fill = {}) {
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
};

/// Explicit specialisation for bool.
///
/// `std::vector<bool>` is a space-optimised bitset with awkward proxy semantics
/// and no stable raw data pointer. We keep an explicit packed bitset instead so
/// bool columns use 1 bit per row while preserving a predictable value API.
template <>
class Column<bool> {
   public:
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

    [[nodiscard]] auto size() const noexcept -> size_type { return size_bits_; }
    [[nodiscard]] auto empty() const noexcept -> bool { return size_bits_ == 0; }
    [[nodiscard]] auto word_count() const noexcept -> size_type { return words_.size(); }

    [[nodiscard]] auto operator[](size_type idx) const noexcept -> bool {
        return (words_[word_index(idx)] & bit_mask(idx)) != 0;
    }
    [[nodiscard]] auto operator[](size_type idx) noexcept -> Reference {
        return Reference(&words_[word_index(idx)], bit_mask(idx));
    }

    auto operator=(const std::vector<bool>& bools) -> Column& {
        clear();
        reserve(bools.size());
        for (const bool v : bools)
            push_back(v);
        return *this;
    }

    auto set(size_type idx, bool value) noexcept -> void {
        auto& word = words_[word_index(idx)];
        const word_type mask = bit_mask(idx);
        if (value) {
            word |= mask;
        } else {
            word &= ~mask;
        }
    }

    void push_back(bool value) {
        const size_type idx = size_bits_;
        if (bit_offset(idx) == 0) {
            words_.push_back(0);
        }
        if (value) {
            words_.back() |= bit_mask(idx);
        }
        ++size_bits_;
    }

    void reserve(size_type n) { words_.reserve(words_for_bits(n)); }

    // zero-initialises (false) for resize-based fill in lag/lead paths
    void resize(size_type n) { resize(n, false); }
    void resize(size_type n, bool value) {
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

    [[nodiscard]] auto words_data() const noexcept -> const word_type* { return words_.data(); }
    [[nodiscard]] auto words_data() noexcept -> word_type* { return words_.data(); }

    void clear() noexcept {
        words_.clear();
        size_bits_ = 0;
    }

    void assign(size_type count, bool value) {
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
};

}  // namespace ibex
