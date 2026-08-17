\name{ibex_read_parquet}
\alias{ibex_read_parquet}
\title{Create a Lazy Ibex Table from a Parquet File}
\usage{
ibex_read_parquet(
  path,
  session = ibex_default_session(),
  name = NULL,
  fallback = c("warn", "error", "collect")
)
}
\arguments{
\item{path}{A local path, HTTPS URL, or S3 URI to a Parquet file.}
\item{session}{A persistent Ibex session. Defaults to the shared session returned by \code{ibex_default_session()}.}
\item{name}{An optional display name used when printing the lazy table.}
\item{fallback}{The policy for unsupported expressions. \code{"warn"} and \code{"collect"} collect the native prefix and replay the complete current verb in local dplyr; \code{"error"} prohibits that boundary.}
}
\value{
An immutable lazy \code{ibex_tbl} backed by the Parquet file.
}
\description{
Activates the bundled Parquet plugin in the supplied session and creates a lazy
Ibex source. Filters and projections remain in Ibex until the result is
collected.
}
\examples{
\dontrun{
library(dplyr)

ibex_read_parquet("trades.parquet") |>
  filter(price > 10) |>
  select(symbol, price) |>
  collect()
}
}
