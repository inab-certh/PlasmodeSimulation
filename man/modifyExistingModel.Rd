% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Models.R
\name{modifyExistingModel}
\alias{modifyExistingModel}
\title{Modify existing model}
\usage{
modifyExistingModel(modelDir, outcomeId, exposureIds, newBetas)
}
\arguments{
\item{modelDir}{The directory where all the outcome models are stored.}

\item{outcomeId}{The outcome id of the predicted by the model being modified}

\item{exposureIds}{The exposure ids for which the model will be modified. If
set to -1, all exposures will be affected the same. Consequently, no
effect modification will be present.}

\item{newBetas}{A list of data frames with columns \code{column_label} and
\code{estimate}. Column \code{column_label} contains the covariate id for
which the coefficient will be replaced by column \code{estimate}. Should
be the same length as \code{exposureIds}, unless \code{exposureIds} is
-1.}
}
\description{
Modify an existing model by updating its coefficients
}
