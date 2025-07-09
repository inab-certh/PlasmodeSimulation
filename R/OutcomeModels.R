fitPoissonRegression <- function(
  connectionDetails,
  resultDatabaseSchema,
  outcomeDatabaseSchema,
  outcomeTable,
  outcomeId,
  exposureDatabaseSchema,
  exposureTable,
  covariateData,
  prior = Cyclops::createPrior(
    priorType = "laplace",
    variance = 0.1
  ),
  control = Cyclops::createControl(
    cvType = "auto",
    maxIterations = 3000,
    threads = -1
  ),
  saveDir
) {

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
  ...
) {

  for (i in seq_along(outcomeIds)) {
    createDirIfNotExists(
      dir = file.path(saveDir, glue::glue("outcome_{ outcomeIds[i] }"))
    )
  }

  sameCovariateSettings <- checkIfUsingSameSettings(
    settings = covariateSettings,
    targetClass = "covariateSettings",
    nOutcomeIds = length(outcomeIds),
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
      fileName = file.path(saveDir, "covariateData")
    )

  } else {
    for (i in seq_along(outcomeIds)) {

      covariateData <- FeatureExtraction::getDbCovariateData(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortTable = exposureTable,
        cohortDatabaseSchema = exposureDatabaseSchema,
        covariateSettings = covariateSettings[[i]]
      )

      Andromeda::saveAndromeda(
        andromeda = covariateData,
        fileName = file.path(
          saveDir,
          glue::glue("outcome_{ outcomeIds[i] }"),
          "covariateData"
        )
      )
    }
  }

  for (i in seq_along(outcomeIds)) {
    covariateDataFile <- findFile(
      "covariateData",
      dirs = c(
        file.path(saveDir, glue::glue("outcome_{ outcomeIds[i] }")),
        saveDir
      )
    )

    covariateData <- Andromeda::loadAndromeda(covariateDataFile)

    result <- fitPoissonRegression(
      connectionDetails = connectionDetails,
      resultDatabaseSchema = resultDatabaseSchema,
      outcomeDatabaseSchema = outcomeDatabaseSchema,
      outcomeTable = outcomeTable,
      outcomeId = outcomeIds[i],
      exposureDatabaseSchema = exposureDatabaseSchema,
      exposureTable = exposureTable,
      covariateData = covariateData,
      saveDir = saveDir,
      ...
    )
  }

  message("Finished training poisson models")

}
