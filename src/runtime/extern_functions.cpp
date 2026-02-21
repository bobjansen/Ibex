#include <ibex/runtime/csv.hpp>
#include <ibex/runtime/extern_functions.hpp>

#include <string>

namespace ibex::runtime {

void register_read_csv(ExternRegistry& registry) {
    registry.register_table(
        "read_csv",
        [](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("read_csv() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_csv() expects a string path");
            }
            auto table = read_csv_simple(*path);
            if (!table) {
                return std::unexpected(table.error());
            }
            return ExternValue{std::move(table.value())};
        });
}

}  // namespace ibex::runtime
