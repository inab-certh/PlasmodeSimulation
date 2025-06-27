generateEventForSubject <- function(
  rowId,
  plpModel,
  covariateData,
  seed
) {

  if (missing(seed)) {
    seed <- sample(1:1e6, 1)
  }

  times <- plpModel$model$baselineSurvival$time
  baselineSurvival <- plpModel$model$baselineSurvival$surv
  idx <- which(!duplicated(baselineSurvival))

  xx <- withr::with_seed(seed, runif(1, 0, 1))
  thisRowId <- rowId

  covariates <- covariateData$covariates |>
    dplyr::collect() |>
    dplyr::filter(rowId == thisRowId)

  nonZeroCoef <- plpModel$model$coefficients |>
    dplyr::filter(betas != 0) |>
    dplyr::mutate(covariateIds = as.numeric(covariateIds))

  predictions <- predictSurvivalOnSubject(
    rowId = thisRowId,
    plpModel = plpModel,
    covariates = covariates,
    nonZeroCoef = nonZeroCoef
  )

  if (xx < dplyr::last(predictions)) {
    result <- Inf
  } else {
    k <- which(predictions < xx) |> min()
    result <- times[idx[k]]
  }

  list(rowId = rowId, time = result)

}

predictSurvivalOnSubject <- function(
  rowId,
  nonZeroCoef,
  covariates
) {


  times <- plpModel$model$baselineSurvival$time
  baselineSurvival <- plpModel$model$baselineSurvival$surv
  idx <- which(!duplicated(baselineSurvival))

  .predict <- function(
    id,
    covariates,
    times,
    baselineSurvival,
    nonZeroCoef
  ) {

    s0 <- baselineSurvival[id]
    survival <- function(x, s0) {
      s0^exp(x)
    }

    dd <- covariates |>
      dplyr::left_join(nonZeroCoef, by = c("covariateId" = "covariateIds")) |>
      dplyr::filter(!is.na(betas))
    if (nrow(dd) == 0) {
      NULL
    } else {

      dd |>
      dplyr::mutate(value = covariateValue * betas) |>
      dplyr::pull(value) |>
      sum() |>
      survival(s0 = s0)

    }

  }

  purrr::map_dbl(
    .x = idx,
    .f = .predict,
    times = times,
    baselineSurvival = baselineSurvival,
    covariates = covariates,
    nonZeroCoef = nonZeroCoef
  )

}

