\name{ibex_default_session}
\alias{ibex_default_session}
\alias{ibex_reset_default_session}
\title{The Shared Session Backing \code{ibex_tbl()}}
\usage{
ibex_default_session()

ibex_reset_default_session()
}
\value{
\code{ibex_default_session} returns an Ibex session handle, creating it on
first use. \code{ibex_reset_default_session} returns \code{NULL} invisibly.
}
\description{
Tables can only be combined natively -- joined, or referenced from the same
query -- when they live in the same session. \code{\link{ibex_tbl}} therefore
binds into one lazily created per-R-session default rather than calling
\code{\link{create_session}} afresh for every table, which would make even
\code{inner_join(ibex_tbl(a), ibex_tbl(b))} fall back to local dplyr.
}
\details{
Bindings in the default session are held for the lifetime of the R session, so
repeatedly wrapping large tables accumulates memory.
\code{ibex_reset_default_session} discards the session, invalidating every
\code{ibex_tbl} created against it; the next \code{ibex_default_session} call
creates a fresh one. Pass an explicit \code{session} to \code{ibex_tbl} when
independent lifetimes or isolation are wanted.
}
\examples{
\dontrun{
library(dplyr)

left <- ibex_tbl(flights)
right <- ibex_tbl(airlines)
# Same session, so this stays native rather than collecting.
joined <- inner_join(left, right, by = "carrier")

ibex_reset_default_session()
}
}
