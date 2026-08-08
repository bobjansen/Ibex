\name{ibex_tbl}
\alias{ibex_tbl}
\title{Create a Lazy dplyr Table Backed by Ibex}
\usage{
ibex_tbl(
  x,
  session = create_session(),
  name = NULL,
  fallback = c("warn", "error", "collect")
)
}
\arguments{
\item{x}{An in-memory data frame, tibble, or nanoarrow-compatible table.}
\item{session}{A persistent \code{ibex} session.}
\item{name}{An optional display name. Native binding names are generated.}
\item{fallback}{The policy for unsupported expressions. \code{"warn"} and
\code{"collect"} collect the native prefix and replay the complete current
verb in local dplyr; \code{"error"} prohibits that boundary.}
}
\value{
An immutable lazy \code{ibex_tbl}. Use \code{dplyr::show_query()} to inspect
generated Ibex, \code{dplyr::compute()} to materialize inside the session, or
\code{dplyr::collect()} to return a tibble.
}
\description{
Binds an in-memory table once and translates a documented subset of dplyr into
an Ibex-native lazy plan.
}
\details{
Captured length-one R values cross as typed scalar bindings and are not
interpolated into source. Unsupported calls are classified when their verb is
constructed. Once a permitted fallback returns a local tibble, later verbs stay
local. Arbitrary R closures never execute as Ibex worker kernels.

The installed \file{dplyr-compatibility.md} file contains the supported verb,
function, type, null, grouping, and ordering matrix.
}
\examples{
\dontrun{
library(dplyr)

query <- ibex_tbl(trades, fallback = "error") |>
  filter(price > 10) |>
  group_by(symbol) |>
  summarise(total = sum(price), .groups = "drop")

show_query(query)
result <- collect(query)
}
}
