#pragma once

#include <ibex/parser/parser.hpp>

#include <cstdlib>
#include <expected>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace ibex::tools {

namespace detail {

inline auto split_path_list(const std::string& raw) -> std::vector<std::filesystem::path> {
    std::vector<std::filesystem::path> out;
    std::size_t start = 0;
    while (start <= raw.size()) {
        std::size_t end = raw.find(':', start);
        if (end == std::string::npos) {
            end = raw.size();
        }
        if (end > start) {
            out.emplace_back(raw.substr(start, end - start));
        }
        start = end + 1;
    }
    return out;
}

inline void push_unique_path(std::vector<std::filesystem::path>& out,
                             const std::filesystem::path& path) {
    if (path.empty()) {
        return;
    }
    auto normalized = path.lexically_normal();
    for (const auto& existing : out) {
        if (existing.lexically_normal() == normalized) {
            return;
        }
    }
    out.push_back(std::move(normalized));
}

inline auto discover_project_root(std::filesystem::path start)
    -> std::optional<std::filesystem::path> {
    namespace fs = std::filesystem;
    if (start.empty()) {
        start = fs::current_path();
    }
    if (!start.empty() && !fs::is_directory(start)) {
        start = start.parent_path();
    }
    while (!start.empty()) {
        if (fs::exists(start / "CMakeLists.txt") && fs::exists(start / "libs")) {
            return start;
        }
        auto parent = start.parent_path();
        if (parent == start) {
            break;
        }
        start = std::move(parent);
    }
    return std::nullopt;
}

inline auto import_search_paths(const std::filesystem::path& entry_file,
                                const std::vector<std::string>& explicit_paths)
    -> std::vector<std::filesystem::path> {
    namespace fs = std::filesystem;
    std::vector<fs::path> paths;

    push_unique_path(paths, entry_file.parent_path());
    push_unique_path(paths, fs::current_path());
    push_unique_path(paths, fs::current_path() / "libs");

    for (const auto& p : explicit_paths) {
        push_unique_path(paths, p);
    }

    if (const char* env = std::getenv("IBEX_LIBRARY_PATH"); env != nullptr && *env != '\0') {
        for (const auto& p : split_path_list(env)) {
            push_unique_path(paths, p);
        }
    }

    if (auto root = discover_project_root(entry_file); root.has_value()) {
        push_unique_path(paths, *root / "libs");
    } else if (auto cwd_root = discover_project_root(fs::current_path()); cwd_root.has_value()) {
        push_unique_path(paths, *cwd_root / "libs");
    }

    return paths;
}

inline auto import_candidates(const std::string& name, const std::filesystem::path& base_dir)
    -> std::vector<std::filesystem::path> {
    std::vector<std::filesystem::path> candidates;
    std::filesystem::path module{name};
    std::filesystem::path direct = module;
    if (!direct.has_extension()) {
        direct += ".ibex";
    }
    candidates.push_back(base_dir / direct);

    const std::string leaf = module.filename().string();
    if (!leaf.empty()) {
        std::string leaf_file = leaf;
        if (!std::filesystem::path(leaf_file).has_extension()) {
            leaf_file += ".ibex";
        }
        candidates.push_back(base_dir / module / leaf_file);
    }
    return candidates;
}

inline auto resolve_import_file(const std::string& name, const std::filesystem::path& current_dir,
                                const std::vector<std::filesystem::path>& search_paths)
    -> std::optional<std::filesystem::path> {
    std::vector<std::filesystem::path> bases;
    push_unique_path(bases, current_dir);
    for (const auto& p : search_paths) {
        push_unique_path(bases, p);
    }

    for (const auto& base : bases) {
        for (const auto& candidate : import_candidates(name, base)) {
            if (std::filesystem::exists(candidate) && std::filesystem::is_regular_file(candidate)) {
                return candidate;
            }
        }
    }
    return std::nullopt;
}

inline auto read_text_file(const std::filesystem::path& path)
    -> std::expected<std::string, std::string> {
    std::ifstream in(path);
    if (!in) {
        return std::unexpected("unable to open '" + path.string() + "'");
    }
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

inline auto stable_path_key(const std::filesystem::path& path) -> std::string {
    try {
        return std::filesystem::weakly_canonical(path).lexically_normal().string();
    } catch (...) {
        return path.lexically_normal().string();
    }
}

inline auto expand_imports_impl(parser::Program program, const std::filesystem::path& current_dir,
                                const std::vector<std::filesystem::path>& search_paths,
                                std::unordered_set<std::string>& imported_paths)
    -> std::expected<parser::Program, std::string> {
    parser::Program out;
    out.statements.reserve(program.statements.size());

    for (auto& stmt : program.statements) {
        if (!std::holds_alternative<parser::ImportDecl>(stmt)) {
            out.statements.push_back(std::move(stmt));
            continue;
        }

        const auto& import = std::get<parser::ImportDecl>(stmt);
        auto resolved = resolve_import_file(import.name, current_dir, search_paths);
        if (!resolved.has_value()) {
            return std::unexpected("import '" + import.name + "': could not find '" + import.name +
                                   ".ibex'");
        }

        const auto key = stable_path_key(*resolved);
        if (imported_paths.contains(key)) {
            continue;
        }
        imported_paths.insert(key);

        auto source = read_text_file(*resolved);
        if (!source.has_value()) {
            return std::unexpected("import '" + import.name + "': " + source.error());
        }
        auto parsed = parser::parse(*source);
        if (!parsed.has_value()) {
            std::ostringstream oss;
            oss << "import '" << import.name << "': parse error at " << resolved->string() << ":"
                << parsed.error().line << ":" << parsed.error().column << ": "
                << parsed.error().message;
            return std::unexpected(oss.str());
        }

        auto expanded = expand_imports_impl(std::move(*parsed), resolved->parent_path(),
                                            search_paths, imported_paths);
        if (!expanded.has_value()) {
            return std::unexpected(expanded.error());
        }

        for (auto& imported_stmt : expanded->statements) {
            out.statements.push_back(std::move(imported_stmt));
        }
    }

    return out;
}

}  // namespace detail

inline auto expand_imports(parser::Program program, const std::string& entry_file,
                           const std::vector<std::string>& explicit_paths = {})
    -> std::expected<parser::Program, std::string> {
    namespace fs = std::filesystem;
    fs::path entry = fs::absolute(fs::path(entry_file));
    auto paths = detail::import_search_paths(entry, explicit_paths);

    std::unordered_set<std::string> imported_paths;
    return detail::expand_imports_impl(std::move(program), entry.parent_path(), paths,
                                       imported_paths);
}

}  // namespace ibex::tools
