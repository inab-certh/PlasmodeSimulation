runSampleGeneration <- function(
  simulationDatabaseDir,
  size,
  exposureIds,
  outcomeIds,
  covariateSettings,
  modelDir,
  saveDir
) {

  sampleData <- Andromeda::andromeda()
  simulationDatabase <- Andromeda::loadAndromeda(simulationDatabaseDir)

  sampleData$sampled_cohorts <- generateSample(
    simulationDatabase = simulationDatabase,
    size = size,
    exposureIds = exposureIds
  ) |>
    dplyr::rename_with(convertToSnakeCase, capitalize = FALSE)

  limitCdmToSample(simulationDatabase, sampleData)

   covariateData <- getSampledCohortsCovariateData(
    sampleData = sampleData,
    covariateSettings = covariateSettings
  )

  sampleData$simulated_outcome <- dplyr::tibble(
    cohort_definition_id = integer(),
    subject_id = integer(),
    cohort_start_date = lubridate::as_date(character()),
    cohort_end_date = lubridate::as_date(character())
  )

  for (i in seq_along(outcomeIds)) {

    message(
      glue::glue(
        "Generating outcome times for outcome: { outcomeIds[i] }"
      )
    )

    thisModel <- readRDS(
      file.path(
        modelDir,
        glue::glue("outcome_{ outcomeIds[i] }"),
        "model.rds"
      )
    )

    linearPredictor <- computePoissonLinearPredictor(
      model = thisModel,
      covariateData = covariateData
    )

    eventTimes <- generatePoissonEventTimes(linearPredictor, 200)


    generateNewOutcomeTable(
      sampleData = sampleData,
      eventTimes = eventTimes,
      outcomeId = outcomeIds[i]
    )

  }

  Andromeda::saveAndromeda(
    andromeda = sampleData,
    fileName = file.path(saveDir, "sampleData.zip")
  )
}

generateSample <- function(
  simulationDatabase,
  size,
  exposureIds
) {

  message("Sampling cohorts...")

  simulationDatabase$exposure |>
    dplyr::filter(cohort_definition_id %in% exposureIds) |>
    dplyr::collect() |>
    dplyr::sample_n(
      size = size,
      replace = TRUE
    ) |> 
    dplyr::rename_with(convertToCamelCase) |>
    dplyr::rename("sourceSubjectId" = "subjectId") |>
    dplyr::arrange(sourceSubjectId) |>
    dplyr::mutate(subjectId = seq_len(dplyr::n())) |>
    dplyr::arrange(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$sourceSubjectId
    ) |>
    dplyr::relocate(subjectId, .before = sourceSubjectId)
}



getSampledCohortsCovariateData <- function(
  sampleData,
  covariateSettings
) {

  connectionDetails <- DatabaseConnector::createDbiConnectionDetails(
    dbms = "duckdb",
    drv = duckdb::duckdb(),
    dbdir = attr(sampleData, "dbname")
  )

  covariateData <- FeatureExtraction::getDbCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortTable = "sampled_cohorts",
    cohortDatabaseSchema = "main",
    covariateSettings = covariateSettings
  )

  covariateData
}




# TODO: implement seed
generatePoissonEventTimes <- function(
  linearPredictor,
  nTimes = 10,
  seed
) {

  n <- nrow(linearPredictor)
  rates <- exp(linearPredictor$linearPredictor)

  times <- matrix(
    floor(rexp(n * nTimes, rate = rep(rates, each = nTimes))),
    nrow = n,
    byrow = TRUE
  )

  linearPredictor |>
    dplyr::mutate(
      draws = split(times, seq_len(n))
    ) |>
    dplyr::select(c("rowId", "draws")) |>
    dplyr::mutate(eventTimes = purrr::map(draws, cumsum)) |>
    dplyr::select(-draws) |>
    tidyr::unnest(eventTimes) |>
    # remove event times after approx 100 years
    dplyr::filter(eventTimes < 365.25 * 100)

}


computePoissonLinearPredictor <- function(model, covariateData) {

  intercept <- model$estimation |>
    dplyr::filter(column_label == 0) |>
    dplyr::pull(estimate)

  intercept <- ifelse(length(intercept == 1), intercept, 0)

  covs_dt <- data.table::as.data.table(
    covariateData$covariates |> dplyr::collect()
  )
  dtCoef <- data.table::as.data.table(model$estimation)
  nonZeroCoef <- dtCoef[dtCoef$estimate != 0, ]

  resDT <- covs_dt[
    nonZeroCoef,
    on = c("covariateId" = "column_label"),
    nomatch = 0L
  ][
    , .(linearPredictor = sum(estimate * covariateValue)),
    by = rowId
  ]

  allRows <- data.table::data.table(rowId = unique(covs_dt$rowId))
  outDT <- base::merge(allRows, resDT, by = "rowId", all.x = TRUE)
  outDT$linearPredictor[is.na(outDT$linearPredictor)] <- 0

  dplyr::as_tibble(outDT) |>
    dplyr::mutate(linearPredictor = linearPredictor + intercept)
}



generateNewOutcomeTable <- function(
  sampleData,
  eventTimes,
  outcomeId
) {


  patients <- sampleData$sampled_cohorts |>
    dplyr::left_join(sampleData$observation_period, by = c("subject_id" = "person_id")) |>
    dplyr::collect() |>
    dplyr::rename_with(convertToCamelCase) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::ends_with("Date"),
        lubridate::ymd
      )
    )

  result <- eventTimes |>
    dplyr::left_join(
      patients |>
        dplyr::select(
          "subjectId",
          "observationPeriodStartDate",
          "observationPeriodEndDate"
        ),
      by = c("rowId" = "subjectId")
    ) |>
    dplyr::mutate(
      eventDate = observationPeriodStartDate + lubridate::days(eventTimes)
    ) |>
    dplyr::filter(
      eventDate < observationPeriodEndDate
    ) |>
    dplyr::select("rowId", "eventDate") |>
    dplyr::rename(
      c(
        "subjectId" = "rowId",
        "cohortStartDate" = "eventDate"
      )
    ) |>
    dplyr::mutate(
      cohortEndDate = cohortStartDate,
      cohortDefinitionId = outcomeId
    ) |>
    dplyr::relocate(cohortDefinitionId) |>
    dplyr::rename_with(convertToSnakeCase, capitalize = FALSE) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::ends_with("date"),
        lubridate::ymd
      )
    )

  Andromeda::appendToTable(sampleData$simulated_outcome, result)


  message("Added lines to table with simulated outcomes")
}
