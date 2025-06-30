generateEventForSubject <- function(
  linearPredictor,
  baselineSurvival,
  seed
) {
  if (missing(seed)) {

    seed <- sample(1:1e6, 1)
  }

  nPatients <- nrow(linearPredictor)
  linearPredictor <- linearPredictor |>
    dplyr::mutate(
      randomUniform = withr::with_seed(
        seed,
        runif(nPatients, 0, 1)
      )
    )

  idx <- which(!duplicated(baselineSurvival$surv))
  eventAtTime <- list()
  survivedAtTime <- linearPredictor
  for (i in seq_along(idx)) {
    remainingAtTime <- survivedAtTime |>
      dplyr::mutate(
        survival = baselineSurvival$surv[idx[i]]^exp(linearPredictor),
        event = survival < randomUniform,
        time = baselineSurvival$time[idx[i]]
      )

    eventAtTime[[i]] <- remainingAtTime |>
      dplyr::filter(event)

    survivedAtTime <- remainingAtTime |>
      dplyr::filter(!event)
  }

  data.table::rbindlist(eventAtTime, use.names = TRUE) |>
    dplyr::as_tibble() |>
    dplyr::bind_rows(survivedAtTime) |>
    dplyr::mutate(time = ifelse(event, time, Inf)) |>
    dplyr::arrange(rowId)
}


computeLinearPredictor <- function(plpModel, covariateData) {
  covs_dt <- data.table::as.data.table(
    covariateData$covariates |> dplyr::collect()
  )
  dtCoef <- data.table::as.data.table(plpModel$model$coefficients)
  dtCoef$covariateIds <- as.numeric(dtCoef$covariateIds)
  nonZeroCoef <- dtCoef[dtCoef$betas != 0, ]

  resDT <- covs_dt[
    nonZeroCoef,
    on = c("covariateId" = "covariateIds"),
    nomatch = 0L
  ][
    , .(linearPredictor = sum(betas * covariateValue)),
    by = rowId
  ]

  allRows <- data.table::data.table(rowId = unique(covs_dt$rowId))
  outDT <- base::merge(allRows, resDT, by = "rowId", all.x = TRUE)
  outDT$linearPredictor[is.na(outDT$linearPredictor)] <- 0

  dplyr::as_tibble(outDT)
}

generateNewExposureTable <- function(
  connectionDetails,
  exposureDatabaseSchema,
  exposureTable,
  resultDatabaseSchema,
  resultTable,
  eventTimes
) {

  TRUE
}
