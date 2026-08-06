\name{ribex-session}
\alias{create_session}
\alias{reset_session}
\alias{session_eval}
\alias{session_eval_file}
\alias{eval_ibex}
\alias{eval_file}
\alias{default_plugin_paths}
\alias{knitr_session}
\alias{register_knitr_engines}
\title{Evaluate Ibex Queries and Manage Persistent Sessions}
\usage{
create_session(plugin_paths = default_plugin_paths())

reset_session(session)

session_eval(session, query, tables = NULL, scalars = NULL,
             format = c("data.frame", "nanoarrow"))

session_eval_file(session, path, tables = NULL, scalars = NULL,
                  format = c("data.frame", "nanoarrow"))

eval_ibex(query, plugin_paths = default_plugin_paths(), tables = NULL,
          scalars = NULL, format = c("data.frame", "nanoarrow"))

eval_file(path, plugin_paths = default_plugin_paths(), tables = NULL,
          scalars = NULL, format = c("data.frame", "nanoarrow"))

default_plugin_paths()

knitr_session(name = "default", plugin_paths = default_plugin_paths())

register_knitr_engines()
}
\arguments{
\item{plugin_paths}{Character vector of directories in which Ibex plugins are
searched. By default, environment-configured paths and conventional build-tree
plugin directories are used.}
\item{session}{A session created by \code{create_session()}.}
\item{query}{One Ibex source string.}
\item{path}{Path to an Ibex source file.}
\item{tables}{Named list of data frames or nanoarrow arrays made available to
the query.}
\item{scalars}{Named list of scalar values made available to the query.}
\item{format}{Result format: a data frame or a nanoarrow array.}
\item{name}{The name of a persistent knitr session.}
}
\value{
\code{create_session()} and \code{knitr_session()} return a persistent Ibex
session. Evaluation functions return \code{NULL} for a query with no value, or
a data frame or nanoarrow array according to \code{format}. \code{reset_session()}
and \code{register_knitr_engines()} return invisibly. \code{default_plugin_paths()}
returns existing plugin directories.
}
\description{
Creates persistent Ibex sessions, evaluates source strings or files, and
optionally exposes an \code{ibex} engine to knitr.
}
\details{
Use a session when several queries share resident table bindings. The one-shot
\code{eval_ibex()} and \code{eval_file()} helpers create the required runtime
state for a single evaluation. R data frames are copied into Ibex; supported
nanoarrow arrays are passed through the Arrow C Data Interface.
}
\examples{
\dontrun{
session <- create_session()
session_eval(session, "let prices = Table { x = [1, 2, 3] };")
session_eval(session, "prices[select { total = sum(x) }];")
}
}
