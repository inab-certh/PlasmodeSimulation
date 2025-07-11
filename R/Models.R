fitPoissonRegression <- function(
  outcomeId,
  covariateData,
  saveDir,
  prior = Cyclops::createPrior(
    priorType = "laplace",
    variance = 0.1
  ),
  control = Cyclops::createControl(
    cvType = "auto",
    maxIterations = 3000,
    threads = -1
  )
) {

  analysisData <- readr::read_csv(
    file = file.path(
      saveDir,
      glue::glue("outcome_{ outcomeId }"),
      "analysisData.csv"
    ),
    show_col_types = FALSE
  )

  rowIds <- covariateData$covariates |>
    dplyr::collect() |>
    dplyr::pull(rowId) |>
    unique()

  outcomes <- dplyr::tibble(rowId = rowIds) |>
    dplyr::left_join(analysisData, by = c("rowId" = "subjectId")) |>
    dplyr::transmute(rowId, y = outcomeCount, time = timeAtRiskDays) |>
    dplyr::filter(time > 0)

  cyclopsData <- Cyclops::convertToCyclopsData(
    outcomes = outcomes,
    covariates = covariateData$covariates |> dplyr::collect(),
    modelType = "pr",
    addIntercept = TRUE
  )

  fit <- Cyclops::fitCyclopsModel(
    cyclopsData = cyclopsData,
    prior = prior,
    control = control,
    warnings = FALSE
  )

  filePath <- file.path(saveDir, glue::glue("outcome_{ outcomeId }"))
  if (!dir.exists(filePath)) dir.create(filePath, recursive = TRUE)
  saveRDS(fit, file.path(filePath, "model.rds"))

  modelOverview <- dplyr::tibble(
    outcomeId = outcomeId,
    location = file.path(filePath, "model.rds"),
    return = stringr::str_to_lower(fit$return_flag)
  )

  readr::write_csv(modelOverview, file.path(filePath, "modelOverview.csv"))

  fit
}

trainPoissonModels <- function(
  connectionDetails,
  cdmDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  outcomeDatabaseSchema,
  outcomeTable,
  outcomeIds,
  resultDatabaseSchema,
  covariateSettings,
  saveDir,
  workers = 1,
  ...
) {

  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)

  for (outcomeId in outcomeIds) {
    createDirIfNotExists(
      dir = file.path(saveDir, glue::glue("outcome_{ outcomeId }"))
    )
  }

  sameCovariateSettings <- checkIfUsingSameSettings(
    settings      = covariateSettings,
    targetClass   = "covariateSettings",
    nOutcomeIds   = length(outcomeIds),
    settingsLabel = "covariate settings"
  )

  if (sameCovariateSettings) {
    covariateData <- FeatureExtraction::getDbCovariateData(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = exposureTable,
      cohortDatabaseSchema = exposureDatabaseSchema,
      covariateSettings = covariateSettings
    )
    Andromeda::saveAndromeda(
      andromeda = covariateData,
      fileName  = file.path(saveDir, "covariateData")
    )
  } else {
    for (i in seq_along(outcomeIds)) {
      outcomeId <- outcomeIds[i]
      covariateData <- FeatureExtraction::getDbCovariateData(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortTable = exposureTable,
        cohortDatabaseSchema = exposureDatabaseSchema,
        covariateSettings = covariateSettings[[i]]
      )
      Andromeda::saveAndromeda(
        andromeda = covariateData,
        fileName  = file.path(
          saveDir,
          glue::glue("outcome_{outcomeId}"), "covariateData")
      )
    }
  }

  message("Extracting analysis data for all models...")
  purrr::walk(
    .x = outcomeIds,
    .f = extractAnalysisData,
    connectionDetails = connectionDetails,
    outcomeDatabaseSchema = outcomeDatabaseSchema,
    outcomeTable = outcomeTable,
    exposureDatabaseSchema = exposureDatabaseSchema,
    exposureTable = exposureTable,
    saveDir = saveDir
  )

  processOutcome <- function(outcomeId) {
    covFile <- findFile(
      "covariateData",
      dirs = c(
        file.path(saveDir, glue::glue("outcome_{outcomeId}")),
        saveDir
      )
    )
    covData <- Andromeda::loadAndromeda(covFile)
    on.exit(Andromeda::close(covData))

    fitPoissonRegression(
      outcomeId = outcomeId,
      covariateData = covData,
      saveDir = saveDir,
      ...
    )
  }

  if (workers > 1) {
    future::plan(future::multisession, workers = workers)
  } else {
    future::plan(future::sequential)
  }

  furrr::future_walk(
    outcomeIds,
    processOutcome,
    .progress = TRUE,
    .options = furrr::furrr_options(seed = TRUE)
  )

  message("Generating overview...")

  files <- list.files(
    path = "models",
    pattern = "modelOverview\\.csv$",
    recursive = TRUE,
    full.names = TRUE
  )

  combined <- files |>
    purrr::set_names() |>
    purrr::map_dfr(
      ~ readr::read_csv(.x, show_col_types = FALSE),
      .id = "path"
    ) |>
    dplyr::mutate(
      outcomeId = path |>
        stringr::str_extract("outcome_[^/]+") |>
        stringr::str_remove("^outcome_")
    ) |>
    dplyr::select(outcomeId, dplyr::everything(), -path)

  readr::write_csv(combined, "models/overview.csv")


  message("Finished training poisson models")
}

extractAnalysisData <- function(
  connectionDetails,
  outcomeDatabaseSchema,
  outcomeTable,
  outcomeId,
  exposureDatabaseSchema,
  exposureTable,
  saveDir
) {

  saveDir <- file.path(
    saveDir,
    glue::glue("outcome_{ outcomeId }")
  )

  createDirIfNotExists(saveDir)

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- glue::glue(
    "
    WITH outcomes AS (
      SELECT *
      FROM { outcomeDatabaseSchema }.{ outcomeTable }
      WHERE cohort_definition_id = { outcomeId }
    )
    SELECT
      c.subject_id,
      COUNT(o.subject_id) outcome_count,
      DATE_DIFF('day', c.cohort_start_date, c.cohort_end_date) time_at_risk_days
    FROM {exposureDatabaseSchema}.{exposureTable} AS c
    LEFT JOIN outcomes AS o
      ON c.subject_id = o.subject_id
     AND o.cohort_start_date BETWEEN c.cohort_start_date AND c.cohort_end_date
    GROUP BY
      c.subject_id,
      c.cohort_start_date,
      c.cohort_end_date
    "
  )

  analysisData <- DatabaseConnector::querySql(connection, sql) |>
    dplyr::rename_with(convertToCamelCase)

  saveFile <- file.path(saveDir, "analysisData.csv")
  readr::write_csv(analysisData, saveFile)
}
