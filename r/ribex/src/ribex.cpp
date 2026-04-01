#define R_NO_REMAP

#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <dlfcn.h>
#include <expected>
#include <filesystem>
#include <fstream>
#include <memory>
#include <R_ext/Error.h>
#include <Rinternals.h>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

auto make_error(std::string_view stage, const std::string& message) -> std::string {
    return "ribex " + std::string(stage) + ": " + message;
}

auto plugin_stem(const std::string& source_path) -> std::string {
    return std::filesystem::path(source_path).stem().string();
}

enum class PluginLoadStatus : std::uint8_t { Loaded, NotFound, LoadError };

struct PluginLoadResult {
    PluginLoadStatus status;
    std::string message;
};

struct SessionState {
    ibex::runtime::TableRegistry tables;
    std::unordered_set<std::string> table_externs;
    std::unordered_set<std::string> sink_externs;
    ibex::runtime::ExternRegistry externs;
    std::unordered_set<std::string> loaded_plugins;
    std::vector<std::string> plugin_paths;
};

auto try_load_plugin(const std::string& stem, const std::vector<std::string>& search_paths,
                     std::unordered_set<std::string>& loaded_plugins,
                     ibex::runtime::ExternRegistry& externs) -> PluginLoadResult {
    if (loaded_plugins.contains(stem)) {
        return {PluginLoadStatus::Loaded, ""};
    }

    const std::string filename = stem + ".so";
    std::string last_error;
    std::string last_candidate;
    for (const auto& dir : search_paths) {
        auto full_path = std::filesystem::path(dir) / filename;
        void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            if (std::filesystem::exists(full_path)) {
                if (const char* err = dlerror()) {
                    last_error = err;
                }
                last_candidate = full_path.string();
            }
            continue;
        }

        using RegisterFn = void (*)(ibex::runtime::ExternRegistry*);
        auto* fn = reinterpret_cast<RegisterFn>(dlsym(handle, "ibex_register"));
        if (fn == nullptr) {
            dlclose(handle);
            last_candidate = full_path.string();
            last_error = "missing ibex_register symbol";
            continue;
        }

        fn(&externs);
        loaded_plugins.insert(stem);
        return {PluginLoadStatus::Loaded, ""};
    }

    if (!last_candidate.empty()) {
        return {PluginLoadStatus::LoadError,
                "failed to load '" + last_candidate +
                    "': " + (last_error.empty() ? "unknown error" : last_error)};
    }

    return {PluginLoadStatus::NotFound, ""};
}

auto load_source_plugins(const ibex::parser::Program& program,
                         const std::vector<std::string>& plugin_search_paths,
                         ibex::runtime::ExternRegistry& externs)
    -> std::expected<void, std::string> {
    std::unordered_set<std::string> loaded_plugins;
    for (const auto& stmt : program.statements) {
        if (auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            if (decl->source_path.empty()) {
                continue;
            }
            auto stem = plugin_stem(decl->source_path);
            auto result = try_load_plugin(stem, plugin_search_paths, loaded_plugins, externs);
            if (result.status == PluginLoadStatus::NotFound) {
                return std::unexpected("could not find plugin '" + stem + ".so' in search path");
            }
            if (result.status == PluginLoadStatus::LoadError) {
                return std::unexpected(result.message);
            }
            continue;
        }

        if (std::holds_alternative<ibex::parser::ImportDecl>(stmt)) {
            return std::unexpected(
                "ribex does not yet support import declarations; use explicit extern fn "
                "declarations with plugin_paths");
        }
    }

    return {};
}

auto read_text_file(const std::string& path) -> std::expected<std::string, std::string> {
    std::ifstream in(path);
    if (!in.is_open()) {
        return std::unexpected(make_error("file error", "failed to open Ibex file '" + path + "'"));
    }

    std::ostringstream buffer;
    buffer << in.rdbuf();
    if (!in.good() && !in.eof()) {
        return std::unexpected(make_error("file error", "failed to read Ibex file '" + path + "'"));
    }
    return buffer.str();
}

auto parse_plugin_paths(SEXP plugin_paths_sexp)
    -> std::expected<std::vector<std::string>, std::string> {
    std::vector<std::string> paths;
    if (plugin_paths_sexp == R_NilValue) {
        return paths;
    }
    if (TYPEOF(plugin_paths_sexp) != STRSXP) {
        return std::unexpected("'plugin_paths' must be a character vector");
    }

    const auto n = Rf_length(plugin_paths_sexp);
    paths.reserve(static_cast<std::size_t>(n));
    for (R_xlen_t i = 0; i < n; ++i) {
        SEXP elt = STRING_ELT(plugin_paths_sexp, i);
        if (elt == NA_STRING) {
            return std::unexpected("'plugin_paths' must not contain NA");
        }
        paths.emplace_back(CHAR(elt));
    }
    return paths;
}

auto eval_table_impl(const std::string& source, const std::vector<std::string>& plugin_search_paths)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(make_error("parse error", parsed.error().format()));
    }

    ibex::runtime::ExternRegistry externs;
    if (!plugin_search_paths.empty()) {
        auto loaded = load_source_plugins(*parsed, plugin_search_paths, externs);
        if (!loaded.has_value()) {
            return std::unexpected(make_error("plugin load error", loaded.error()));
        }
    }

    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered.has_value()) {
        return std::unexpected(make_error("lowering error", lowered.error().message));
    }

    ibex::runtime::TableRegistry registry;
    auto evaluated = ibex::runtime::interpret(*lowered.value(), registry, nullptr,
                                              plugin_search_paths.empty() ? nullptr : &externs);
    if (!evaluated.has_value()) {
        return std::unexpected(make_error("runtime error", evaluated.error()));
    }

    return std::make_shared<ibex::runtime::Table>(std::move(*evaluated));
}

auto register_extern_decl(const ibex::parser::ExternDecl& decl, SessionState& session)
    -> std::expected<void, std::string> {
    if (decl.return_type.kind == ibex::parser::Type::Kind::DataFrame ||
        decl.return_type.kind == ibex::parser::Type::Kind::TimeFrame) {
        session.table_externs.insert(decl.name);
    }
    if (!decl.params.empty() && decl.params[0].type.kind == ibex::parser::Type::Kind::DataFrame) {
        session.sink_externs.insert(decl.name);
    }
    if (decl.source_path.empty()) {
        return {};
    }

    auto stem = plugin_stem(decl.source_path);
    auto result =
        try_load_plugin(stem, session.plugin_paths, session.loaded_plugins, session.externs);
    if (result.status == PluginLoadStatus::NotFound) {
        return std::unexpected("could not find plugin '" + stem + ".so' in search path");
    }
    if (result.status == PluginLoadStatus::LoadError) {
        return std::unexpected(result.message);
    }
    return {};
}

auto eval_table_in_session(SessionState& session, const std::string& source)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(make_error("parse error", parsed.error().format()));
    }

    std::shared_ptr<const ibex::runtime::Table> last_table;
    for (const auto& stmt : parsed->statements) {
        if (const auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            auto registered = register_extern_decl(*decl, session);
            if (!registered.has_value()) {
                return std::unexpected(make_error("plugin load error", registered.error()));
            }
            continue;
        }
        if (std::holds_alternative<ibex::parser::ImportDecl>(stmt)) {
            return std::unexpected(
                make_error("session error",
                           "import declarations are not supported in ribex sessions; use "
                           "explicit extern fn declarations"));
        }
        if (std::holds_alternative<ibex::parser::FunctionDecl>(stmt)) {
            return std::unexpected(make_error(
                "session error", "function declarations are not supported in ribex sessions"));
        }
        if (std::holds_alternative<ibex::parser::TupleLetStmt>(stmt)) {
            return std::unexpected(make_error(
                "session error", "tuple let bindings are not supported in ribex sessions"));
        }

        ibex::parser::LowerContext context;
        context.table_externs = session.table_externs;
        context.sink_externs = session.sink_externs;

        if (const auto* let_stmt = std::get_if<ibex::parser::LetStmt>(&stmt)) {
            auto lowered = ibex::parser::lower_expr(*let_stmt->value, context);
            if (!lowered.has_value()) {
                return std::unexpected(
                    make_error("lowering error",
                               "ribex sessions currently support only table-valued let bindings: " +
                                   lowered.error().message));
            }
            ibex::runtime::ScalarRegistry scalars;
            auto evaluated = ibex::runtime::interpret(*lowered.value(), session.tables, &scalars,
                                                      &session.externs);
            if (!evaluated.has_value()) {
                return std::unexpected(make_error("runtime error", evaluated.error()));
            }
            session.tables.insert_or_assign(let_stmt->name, std::move(*evaluated));
            continue;
        }

        const auto& expr_stmt = std::get<ibex::parser::ExprStmt>(stmt);
        auto lowered = ibex::parser::lower_expr(*expr_stmt.expr, context);
        if (!lowered.has_value()) {
            return std::unexpected(
                make_error("lowering error",
                           "ribex sessions currently support only table-valued expressions: " +
                               lowered.error().message));
        }
        ibex::runtime::ScalarRegistry scalars;
        auto evaluated =
            ibex::runtime::interpret(*lowered.value(), session.tables, &scalars, &session.externs);
        if (!evaluated.has_value()) {
            return std::unexpected(make_error("runtime error", evaluated.error()));
        }
        last_table = std::make_shared<ibex::runtime::Table>(std::move(*evaluated));
    }

    return last_table;
}

void schema_finalizer(SEXP ext) {
    auto* schema = static_cast<ArrowSchema*>(R_ExternalPtrAddr(ext));
    if (schema == nullptr) {
        return;
    }
    ibex::interop::release_arrow_schema(schema);
    delete schema;
    R_ClearExternalPtr(ext);
}

void array_finalizer(SEXP ext) {
    auto* array = static_cast<ArrowArray*>(R_ExternalPtrAddr(ext));
    if (array == nullptr) {
        return;
    }
    ibex::interop::release_arrow_array(array);
    delete array;
    R_ClearExternalPtr(ext);
}

void session_finalizer(SEXP ext) {
    auto* session = static_cast<SessionState*>(R_ExternalPtrAddr(ext));
    if (session == nullptr) {
        return;
    }
    delete session;
    R_ClearExternalPtr(ext);
}

auto make_nanoarrow_xptr(void* ptr, SEXP tag, R_CFinalizer_t finalizer, const char* class_name)
    -> SEXP {
    SEXP ext = PROTECT(R_MakeExternalPtr(ptr, tag, R_NilValue));
    R_RegisterCFinalizerEx(ext, finalizer, TRUE);
    SEXP cls = PROTECT(Rf_mkString(class_name));
    Rf_classgets(ext, cls);
    UNPROTECT(2);
    return ext;
}

auto export_table_payload(std::shared_ptr<const ibex::runtime::Table> table)
    -> std::expected<SEXP, std::string> {
    auto schema = std::make_unique<ArrowSchema>();
    auto array = std::make_unique<ArrowArray>();
    auto exported =
        ibex::interop::export_table_to_arrow(std::move(table), array.get(), schema.get());
    if (!exported.has_value()) {
        return std::unexpected(exported.error());
    }

    SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, 2));

    SET_STRING_ELT(names, 0, Rf_mkChar("array"));
    SET_STRING_ELT(names, 1, Rf_mkChar("schema"));
    Rf_setAttrib(out, R_NamesSymbol, names);

    SET_VECTOR_ELT(out, 0,
                   make_nanoarrow_xptr(array.release(), Rf_install("nanoarrow_array"),
                                       array_finalizer, "nanoarrow_array"));
    SET_VECTOR_ELT(out, 1,
                   make_nanoarrow_xptr(schema.release(), Rf_install("nanoarrow_schema"),
                                       schema_finalizer, "nanoarrow_schema"));

    UNPROTECT(2);
    return out;
}

auto scalar_string(SEXP value, const char* what) -> std::expected<std::string, std::string> {
    if (TYPEOF(value) != STRSXP || Rf_length(value) != 1 || STRING_ELT(value, 0) == NA_STRING) {
        return std::unexpected(std::string(what) + " must be a length-1 character value");
    }
    return std::string(CHAR(STRING_ELT(value, 0)));
}

auto session_from_sexp(SEXP session_sexp) -> std::expected<SessionState*, std::string> {
    if (TYPEOF(session_sexp) != EXTPTRSXP) {
        return std::unexpected("'session' must be a ribex session");
    }
    auto* session = static_cast<SessionState*>(R_ExternalPtrAddr(session_sexp));
    if (session == nullptr) {
        return std::unexpected("invalid ribex session");
    }
    return session;
}

}  // namespace

extern "C" SEXP ribex_c_eval_ibex(SEXP query_sexp, SEXP plugin_paths_sexp) {
    auto query = scalar_string(query_sexp, "'query'");
    if (!query.has_value()) {
        Rf_error("%s", query.error().c_str());
    }

    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }

    auto evaluated = eval_table_impl(*query, *plugin_paths);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }

    return *payload;
}

extern "C" SEXP ribex_c_eval_file(SEXP path_sexp, SEXP plugin_paths_sexp) {
    auto path = scalar_string(path_sexp, "'path'");
    if (!path.has_value()) {
        Rf_error("%s", path.error().c_str());
    }

    auto source = read_text_file(*path);
    if (!source.has_value()) {
        Rf_error("%s", source.error().c_str());
    }

    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }

    auto evaluated = eval_table_impl(*source, *plugin_paths);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }

    return *payload;
}

extern "C" SEXP ribex_c_create_session(SEXP plugin_paths_sexp) {
    auto plugin_paths = parse_plugin_paths(plugin_paths_sexp);
    if (!plugin_paths.has_value()) {
        Rf_error("%s", plugin_paths.error().c_str());
    }

    auto* session = new SessionState();
    session->plugin_paths = std::move(*plugin_paths);

    SEXP ext = PROTECT(R_MakeExternalPtr(session, Rf_install("ribex_session"), R_NilValue));
    R_RegisterCFinalizerEx(ext, session_finalizer, TRUE);
    SEXP cls = PROTECT(Rf_mkString("ribex_session"));
    Rf_classgets(ext, cls);
    UNPROTECT(2);
    return ext;
}

extern "C" SEXP ribex_c_reset_session(SEXP session_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }

    auto plugin_paths = (*session)->plugin_paths;
    delete *session;
    auto* fresh = new SessionState();
    fresh->plugin_paths = std::move(plugin_paths);
    R_SetExternalPtrAddr(session_sexp, fresh);
    return session_sexp;
}

extern "C" SEXP ribex_c_session_eval_ibex(SEXP session_sexp, SEXP query_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }

    auto query = scalar_string(query_sexp, "'query'");
    if (!query.has_value()) {
        Rf_error("%s", query.error().c_str());
    }

    auto evaluated = eval_table_in_session(**session, *query);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }
    if (!*evaluated) {
        return R_NilValue;
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }
    return *payload;
}

extern "C" SEXP ribex_c_session_eval_file(SEXP session_sexp, SEXP path_sexp) {
    auto session = session_from_sexp(session_sexp);
    if (!session.has_value()) {
        Rf_error("%s", session.error().c_str());
    }

    auto path = scalar_string(path_sexp, "'path'");
    if (!path.has_value()) {
        Rf_error("%s", path.error().c_str());
    }

    auto source = read_text_file(*path);
    if (!source.has_value()) {
        Rf_error("%s", source.error().c_str());
    }

    auto evaluated = eval_table_in_session(**session, *source);
    if (!evaluated.has_value()) {
        Rf_error("%s", evaluated.error().c_str());
    }
    if (!*evaluated) {
        return R_NilValue;
    }

    auto payload = export_table_payload(std::move(*evaluated));
    if (!payload.has_value()) {
        Rf_error("%s", payload.error().c_str());
    }
    return *payload;
}
