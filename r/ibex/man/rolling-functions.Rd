\name{rolling-functions}
\alias{rolling_sum}
\alias{rolling_mean}
\alias{rolling_min}
\alias{rolling_max}
\alias{rolling_count}
\alias{rolling_median}
\alias{rolling_std}
\alias{rolling_ewma}
\alias{rolling_quantile}
\alias{rolling_skew}
\alias{rolling_kurtosis}
\alias{rolling_first}
\alias{rolling_last}
\alias{window_start}
\alias{window_end}
\title{Ibex rolling expressions}
\usage{
rolling_sum(x, ...)
rolling_mean(x, ...)
rolling_min(x, ...)
rolling_max(x, ...)
rolling_count()
rolling_median(x, ...)
rolling_std(x, ...)
rolling_ewma(x, alpha)
rolling_quantile(x, p)
rolling_skew(x, ...)
rolling_kurtosis(x, ...)
rolling_first(x, ...)
rolling_last(x, ...)
window_start()
window_end()
}
\arguments{
  \item{x}{A column expression.}
  \item{...}{An optional numeric count window where supported.}
  \item{alpha}{The EWMA smoothing parameter.}
  \item{p}{The quantile probability.}
}
\description{
Expression markers for lazy \code{ibex_tbl} plans. Use them inside
\code{dplyr::mutate()} or \code{dplyr::summarise()} after \code{window()}, or
provide a supported numeric per-call count window. They are not ordinary vectorised R
functions and intentionally error when evaluated locally.
}
