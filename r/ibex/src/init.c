#define R_NO_REMAP

#include <R_ext/Rdynload.h>
#include <Rinternals.h>

SEXP ibex_c_eval_file(SEXP path_sexp, SEXP plugin_paths_sexp, SEXP tables_sexp, SEXP scalars_sexp);
SEXP ibex_c_eval_ibex(SEXP query_sexp, SEXP plugin_paths_sexp, SEXP tables_sexp,
                       SEXP scalars_sexp);
SEXP ibex_c_arrow_buffer_addresses(SEXP array_sexp);
SEXP ibex_c_create_session(SEXP plugin_paths_sexp);
SEXP ibex_c_reset_session(SEXP session_sexp);
SEXP ibex_c_session_eval_ibex(SEXP session_sexp, SEXP query_sexp, SEXP tables_sexp,
                               SEXP scalars_sexp);
SEXP ibex_c_session_eval_file(SEXP session_sexp, SEXP path_sexp, SEXP tables_sexp,
                               SEXP scalars_sexp);
SEXP ibex_c_session_table_info(SEXP session_sexp, SEXP name_sexp);
SEXP ibex_c_session_infer_schema(SEXP session_sexp, SEXP query_sexp, SEXP lexical_names_sexp);

static const R_CallMethodDef call_methods[] = {
    {"ibex_c_arrow_buffer_addresses", (DL_FUNC)&ibex_c_arrow_buffer_addresses, 1},
    {"ibex_c_create_session", (DL_FUNC)&ibex_c_create_session, 1},
    {"ibex_c_eval_file", (DL_FUNC)&ibex_c_eval_file, 4},
    {"ibex_c_eval_ibex", (DL_FUNC)&ibex_c_eval_ibex, 4},
    {"ibex_c_reset_session", (DL_FUNC)&ibex_c_reset_session, 1},
    {"ibex_c_session_eval_file", (DL_FUNC)&ibex_c_session_eval_file, 4},
    {"ibex_c_session_eval_ibex", (DL_FUNC)&ibex_c_session_eval_ibex, 4},
    {"ibex_c_session_infer_schema", (DL_FUNC)&ibex_c_session_infer_schema, 3},
    {"ibex_c_session_table_info", (DL_FUNC)&ibex_c_session_table_info, 2},
    {NULL, NULL, 0},
};

void R_init_ibex(DllInfo* dll) {
    R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
