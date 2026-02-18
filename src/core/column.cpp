#include <ibex/core/column.hpp>

#include <cstdint>

// Column<T> is fully header-only (template class).
// This translation unit exists to verify the header compiles
// and to anchor explicit instantiations if needed in the future.

namespace ibex {

// Explicit instantiations for common types to reduce compile times.
template class Column<int>;
template class Column<std::int64_t>;
template class Column<double>;

}  // namespace ibex
