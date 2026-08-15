\name{timeframe-window-resample}
\alias{as_timeframe}
\alias{as_timeframe.ibex_tbl}
\alias{window}
\alias{window.ibex_tbl}
\alias{resample}
\alias{resample.ibex_tbl}
\title{Time-indexed window and resample operations}
\usage{
as_timeframe(.data, index)
window(x, duration, aligned = FALSE, ...)
resample(.data, duration, ..., .by = NULL, aligned = FALSE)
}
\arguments{
  \item{.data, x}{An \code{ibex_tbl}.}
  \item{index}{One Timestamp, Date, or integer-nanosecond column.}
  \item{duration}{An Ibex duration string such as \code{"5m"} or \code{"250ms"}.}
  \item{aligned}{Whether a window resets on epoch-aligned duration boundaries. Resample bars are always aligned.}
  \item{...}{For \code{resample()}, named aggregate expressions; for \code{window()}, no additional arguments.}
  \item{.by}{Optional grouping columns for independent resample bars.}
}
\description{
\code{as_timeframe()} designates a time index and keeps the resulting Ibex plan
lazy. \code{window()} applies a duration to the next \code{mutate()} or
\code{summarise()}, where Ibex rolling helpers such as \code{rolling_mean()}
are available. \code{resample()} creates one aggregate row per time bucket.
}
\examples{
ticks <- ibex_tbl(data.frame(ts = as.POSIXct(c(0, 60), origin = "1970-01-01"),
                              price = c(10, 20)))
ticks |> as_timeframe(ts) |> window("2m") |> dplyr::mutate(avg = rolling_mean(price))
ticks |> as_timeframe(ts) |> resample("1m", close = dplyr::last(price))
}
