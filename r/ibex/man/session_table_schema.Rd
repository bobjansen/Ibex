\name{session_table_schema}
\alias{session_table_schema}
\title{Inspect an Ibex Session Table Schema}
\usage{
session_table_schema(session, name)
}
\arguments{
\item{session}{A persistent \code{ibex} session.}
\item{name}{The name of a resident table binding.}
}
\value{
A tibble with ordered \code{name}, \code{type}, \code{nullable},
\code{categorical}, and \code{timezone} columns. The \code{rows},
\code{ordering}, \code{time_index}, \code{generation}, and \code{grouped_by}
metadata are attached as attributes.
}
\description{
Returns table schema and ordering metadata without executing a user-visible
query.
}
