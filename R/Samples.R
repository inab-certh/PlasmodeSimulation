#' Run sample generation
#'
#' @description Generates a simulated cdm database.
#'
#' @param simulationDatabaseDir The directory where a self-contained database
#'   limited to the target patient cohorts is stored.
#' @param size The size of the simulated database.
#' @param exposureIds The cohort defintion ids of the exposure cohorts of
#'   interest. These are located in the combined cohort table of the limited cdm
#'   database.
#' @param outcomeIds The cohort defintion ids of the outcome cohorts of
#'   interest. These are located in the outcome table of the limited cdm
#'   database.
#' @param covariateSettings An object of type \code{covariateSettings} generated
#'   with \code{\link[FeatureExtraction]{createCovariateSettings}}.
#' @param modelDir The directory where the outcome models are saved.
#' @param saveDir The directory where the simulated database will be stored.
#'
#' @export
 runSampleGeneration <- function(
  simulationDatabaseDir,
  size,
  exposureIds,
  outcomeIds,
  covariateSettings,
  modelDir,
  saveDir
) {

  overview <- readr::read_csv(
    file.path(modelDir, "overview.csv"),
    show_col_types = FALSE
  )
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

  covariates <- covariateData$covariates |> dplyr::collect()

  .generateOutcomes <- function(
    outcomeId,
    overview,
    covariates,
    sampledCohorts,
    observationPeriod
  ) {
    thisModel <- overview |>
      dplyr::filter(.data$outcomeId == !!outcomeId) |>
      dplyr::pull(.data$location) |>
      readr::read_csv(show_col_types = FALSE)

    linearPredictor <- thisModel |>
      dplyr::group_by(.data$exposureId) |>
      tidyr::nest() |>
      dplyr::mutate(
        linearPredictor = purrr::map(
          .x = .data$data,
          .f = \(x) {
            computePoissonLinearPredictor(x, covariates)
          }
        )
      ) |>
      dplyr::select(-"data") |>
      tidyr::unnest("linearPredictor") |>
      dplyr::ungroup("exposureId")


    eventTimes <- linearPredictor |>
      dplyr::group_by(.data$exposureId) |>
      tidyr::nest() |>
      dplyr::mutate(
        eventTimes = purrr::map(
          .x = .data$data,
          .f = \(x) {
            generatePoissonEventTimes(x, 200)
          }
        )
      ) |>
      dplyr::select(-"data") |>
      tidyr::unnest(.data$eventTimes) |>
      dplyr::ungroup(.data$exposureId)

    generateNewOutcomeTable(
      sampledCohorts = sampledCohorts,
      observationPeriod = observationPeriod,
      eventTimes = eventTimes,
      outcomeId = outcomeId
    )
  }

  message("Simulating outcome times. This might take a while...")

  result <- outcomeIds |>
    purrr::map(
      purrr::in_parallel(
        \(x) {
          library(PlasmodeSimulation)
          .generateOutcomes(
            outcomeId = x,
            overview = overview,
            covariates = covariates,
            observationPeriod = observationPeriod,
            sampledCohorts = sampledCohorts
          )
        },
        .generateOutcomes = .generateOutcomes,
        overview = overview,
        covariates = covariates,
        observationPeriod = sampleData$observation_period |> dplyr::collect(),
        sampledCohorts = sampleData$sampled_cohorts |> dplyr::collect()
      ),
      .progress = TRUE
    ) |>
    dplyr::bind_rows()

  Andromeda::appendToTable(sampleData$simulated_outcome, result)

  message("Simulated outcome times")

  Andromeda::saveAndromeda(
    andromeda = sampleData,
    fileName = file.path(saveDir, "sampleData.zip")
  )

  message(
    glue::glue(
      "Stored simulated database in {file.path(saveDir, 'sampleData.zip')}"
    )
  )

  message("Simulation completed successfully")
}

generateSample <- function(
  simulationDatabase,
  size,
  exposureIds
) {

  message("Sampling cohorts...")

  simulationDatabase$exposure |>
    dplyr::filter(.data$cohort_definition_id %in% exposureIds) |>
    dplyr::collect() |>
    dplyr::sample_n(
      size = size,
      replace = TRUE
    ) |> 
    dplyr::rename_with(convertToCamelCase) |>
    dplyr::rename("sourceSubjectId" = "subjectId") |>
    dplyr::arrange(.data$sourceSubjectId) |>
    dplyr::mutate(subjectId = seq_len(dplyr::n())) |>
    dplyr::arrange(
      .data$cohortDefinitionId,
      .data$subjectId,
      .data$sourceSubjectId
    ) |>
    dplyr::relocate(.data$subjectId, .before = .data$sourceSubjectId)
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
    floor(stats::rexp(n * nTimes, rate = rep(rates, each = nTimes))),
    nrow = n,
    byrow = TRUE
  )

  linearPredictor |>
    dplyr::mutate(
      draws = split(times, seq_len(n))
    ) |>
    dplyr::select(c("rowId", "draws")) |>
    dplyr::mutate(eventTimes = purrr::map(.data$draws, cumsum)) |>
    dplyr::select(-"draws") |>
    tidyr::unnest(.data$eventTimes) |>
    # remove event times after approx 100 years
    dplyr::filter(.data$eventTimes < 365.25 * 100)

}


computePoissonLinearPredictor <- function(model, covariates) {

  intercept <- model |>
    dplyr::filter(column_label == 0) |>
    dplyr::pull(estimate)

  intercept <- ifelse(length(intercept == 1), intercept, 0)

  # covs_dt <- data.table::as.data.table(
  #   covariateData$covariates |> dplyr::collect()
  # )
  covs_dt <- data.table::as.data.table(covariates)
  dtCoef <- data.table::as.data.table(model)
  nonZeroCoef <- dtCoef[dtCoef$estimate != 0, ]

  resDT <- covs_dt[
    nonZeroCoef,
    on = c("covariateId" = "column_label"),
    nomatch = 0L
  ][
    , list(linearPredictor = sum(estimate * covariateValue)),
    by = rowId
  ]

  allRows <- data.table::data.table(rowId = unique(covs_dt$rowId))
  outDT <- base::merge(allRows, resDT, by = "rowId", all.x = TRUE)
  outDT$linearPredictor[is.na(outDT$linearPredictor)] <- 0

  dplyr::as_tibble(outDT) |>
    dplyr::mutate(linearPredictor = linearPredictor + intercept)
}

utils::globalVariables(
  c("column_label", "estimate", "covariateValue", "rowId", "linearPredictor")
)


#' @importFrom dplyr .data
generateNewOutcomeTable <- function(
  sampledCohorts,
  observationPeriod,
  eventTimes,
  outcomeId
) {


  patients <- sampledCohorts |>
    dplyr::left_join(
      observationPeriod,
      by = c("subject_id" = "person_id")
    ) |>
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
          "cohortDefinitionId",
          "cohortStartDate",
          "cohortEndDate",
          "observationPeriodStartDate",
          "observationPeriodEndDate"
        ),
      by = c("rowId" = "subjectId")
    ) |>
    dplyr::filter(
      .data$exposureId == -1 |
        .data$exposureId == .data$cohortDefinitionId
    ) |>
    dplyr::mutate(
      eventDate = as.Date(
        ifelse(
          .data$exposureId == -1,
          .data$observationPeriodStartDate + lubridate::days(.data$eventTimes),
          .data$cohortStartDate + lubridate::days(.data$eventTimes)
        )
      )
    ) |>
    dplyr::filter(.data$eventDate <= .data$observationPeriodEndDate) |>
    dplyr::select(-"cohortStartDate") |>
    dplyr::rename(
      c(
        "subjectId" = "rowId",
        "cohortStartDate" = "eventDate"
      )
    ) |>
    dplyr::mutate(
      cohortEndDate = .data$cohortStartDate,
      cohortDefinitionId = outcomeId
    ) |>
    dplyr::select(
      c(
        "cohortDefinitionId",
        "subjectId",
        "cohortStartDate",
        "cohortEndDate"
      )
    ) |>
    dplyr::rename_with(convertToSnakeCase, capitalize = FALSE) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::ends_with("date"),
        lubridate::ymd
      )
    )

  result
}
