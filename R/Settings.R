checkIfUsingSameSettings <- function(
  settings,
  targetClass,
  nOutcomeIds,
  settingsLabel = "label"
) {

  if (inherits(settings, targetClass)) {
    message(glue::glue("Using same {settingsLabel} for all outcomes"))
    return(TRUE)
  } else if (is.list(settings)) {
    if (length(settings) == nOutcomeIds) {
      message(glue::glue("Using different {settingsLabel} for each outcome"))
      return(FALSE)
    } else {
        stop(
          glue::glue(
            "Length of {settingsLabel} list does not match number of outcomes"
          )
      )
    }
  }

  stop(glue::glue("{settingsLabel} must be a list or a '{targetClass}' object"))
}

createModelSettings <- function(
  nfolds = 10,
  foldid = NULL,
  keep = FALSE,
  grouped = TRUE,
  alpha = 1,
  lambda = NULL,
  nlambda = 100,
  lambda.min.ratio = NULL,
  family = "gaussian",
  standardize = TRUE,
  intercept = TRUE,
  thresh = 1e-07,
  maxit = 100000,
  type.measure = "default",
  weights = NULL,
  offset = NULL,
  lower.limits = -Inf,
  upper.limits = Inf,
  penalty.factor = NULL,
  exclude = NULL,
  dfmax = NULL,
  pmax = NULL,
  parallel = FALSE,
  trace.it = 0,
  ...
) {

  settings <- list(
    nfolds = nfolds,
    foldid = foldid,
    keep = keep,
    grouped = grouped,
    type.measure = type.measure,
    weights = weights,
    parallel = parallel,
    trace.it = trace.it,
    alpha = alpha,
    lambda = lambda,
    nlambda = nlambda,
    family = family,
    standardize = standardize,
    intercept = intercept,
    thresh = thresh,
    maxit = maxit,
    offset = offset,
    lower.limits = lower.limits,
    upper.limits = upper.limits,
    penalty.factor = penalty.factor,
    exclude = exclude,
    dfmax = dfmax,
    pmax = pmax
  )

  if (!missing(lambda.min.ratio)) {
    settings$lambda.min.ratio <- lambda.min.ratio
  }

  dots <- list(...)
  if (length(dots) > 0) {
    settings <- c(settings, dots)
  }

  settings <- settings[!sapply(settings, is.null)]

  return(settings)
}
