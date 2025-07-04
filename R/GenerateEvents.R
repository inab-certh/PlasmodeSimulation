runSampleGeneration <- function(
  connectionDetails,
  size,
  cdmDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  exposureIds,
  resultDatabaseSchema,
  simulatedExposureTable,
  simulatedOutcomeTable,
  outcomeModelSettings,
  censoringModelSettings,
  modelDir,
  saveDir
) {

  if (missing(simulatedExposureTable)) {
    simulatedExposureTable <- "simulated_exposures"
  }

  if (missing(simulatedOutcomeTable)) {
    simulatedOutcomeTable <- "simulated_outcomes"
  }

  if (missing(exposureIds)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    exposureIds <- DatabaseConnector::querySql(
      connection = connection,
      sql = glue::glue(
        "
        SELECT DISTINCT cohort_definition_id
        FROM { exposureDatabaseSchema }.{ exposureTable }
        "
      )
    ) |>
      dplyr::arrange(COHORT_DEFINITION_ID) |>
      dplyr::pull(COHORT_DEFINITION_ID)
    DatabaseConnector::disconnect(connection)
  }

  dirsToCreate <- c(
    file.path(saveDir, "censoring"),
    file.path(saveDir, "outcomes")
  )

  purrr::walk(dirsToCreate, \(x) {
    if (!dir.exists(x)) {
      dir.create(x, recursive = TRUE)
      message("Created directory ", x)
    }
  })

  generateSample(
    connectionDetails = connectionDetails,
    size = size,
    exposureDatabaseSchema = exposureDatabaseSchema,
    exposureTable = exposureTable,
    exposureIds = exposureIds,
    resultDatabaseSchema = resultDatabaseSchema,
    simulatedCohortTable = "sampled_cohorts"
  )

  overview <- readr::read_csv(
    file = file.path(modelDir, "overview.csv"),
    show_col_types = FALSE
  )

  validOutcomeIds <- overview |>
    dplyr::filter(checkPassed, model == "outcome") |>
    dplyr::pull(outcomeId) |>
    unique()

  modelsToFit <- overview |>
    dplyr::filter(checkPassed) |>
    dplyr::filter(outcomeId %in% validOutcomeIds)

  covariateData <- getSampledCohortsCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "sampled_cohorts",
    cohortDatabaseSchema = resultDatabaseSchema,
    covariateSettings = outcomeModelSettings$covariateSettings
  )

  Andromeda::saveAndromeda(
    andromeda = covariateData,
    fileName = file.path(saveDir, "outcomes", "covariateData"),
    maintainConnection = TRUE
  )

  analysisIds <- modelsToFit |>
    dplyr::filter(model == "outcome") |>
    dplyr::pull(analysisId)

  connection <- DatabaseConnector::connect(connectionDetails)
  tableNames <- DatabaseConnector::getTableNames(connection)

  if (simulatedOutcomeTable %in% tableNames) {
    DatabaseConnector::executeSql(
      connection = connection,
      sql = glue::glue(
        "DROP TABLE { resultDatabaseSchema }.{ simulatedOutcomeTable };"
      )
    )
    message("Dropped old table with simulated outcomes")
  }

  if (simulatedExposureTable %in% tableNames) {
    DatabaseConnector::executeSql(
      connection = connection,
      sql = glue::glue(
        "DROP TABLE { resultDatabaseSchema }.{ simulatedExposureTable };"
      )
    )
    message("Dropped old table with simulated exposures")
  }
  DatabaseConnector::disconnect(connection)

  for (i in seq_along(analysisIds)) {

    message(
      glue::glue(
        "Generating outcome times for analysis: { analysisIds[i] }"
      )
    )

    thisModelDir <- modelsToFit |>
      dplyr::filter(model == "outcome", analysisId == analysisIds[i]) |>
      dplyr::pull(directory)

    outcomePlpModel <- PatientLevelPrediction::loadPlpModel(
      file.path(thisModelDir, "plpResult", "model")
    )

    outcomeLinearPredictor <- computeLinearPredictor(
      plpModel = outcomePlpModel,
      covariateData = covariateData
    )

    outcomeTimes <- generateEventForSubject(
      baselineSurvival = outcomePlpModel$model$baselineSurvival,
      linearPredictor = outcomeLinearPredictor
    )

    generateNewOutcomeTable(
      connectionDetails = connectionDetails,
      exposureDatabaseSchema = resultDatabaseSchema,
      exposureTable = "sampled_cohorts",
      resultDatabaseSchema = resultDatabaseSchema,
      resultTable = simulatedOutcomeTable,
      eventTimes = outcomeTimes
    )

  }

  covariateData <- getSampledCohortsCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "sampled_cohorts",
    cohortDatabaseSchema = resultDatabaseSchema,
    covariateSettings = censoringModelSettings$covariateSettings
  )

  Andromeda::saveAndromeda(
    andromeda = covariateData,
    fileName = file.path(saveDir, "censoring", "covariateData"),
    maintainConnection = TRUE
  )

  analysisIds <- modelsToFit |>
    dplyr::filter(model == "censoring") |>
    dplyr::pull(analysisId)

  for (i in seq_along(analysisIds)) {

    message(
      glue::glue(
        "Generating censoring times for analysis: { analysisIds[i] }"
      )
    )

    thisModelDir <- modelsToFit |>
      dplyr::filter(model == "censoring", analysisId == analysisIds[i]) |>
      dplyr::pull(directory)

    censoringPlpModel <- PatientLevelPrediction::loadPlpModel(
      file.path(thisModelDir, "plpResult", "model")
    )

    censoringLinearPredictor <- computeLinearPredictor(
      plpModel = censoringPlpModel,
      covariateData = covariateData
    )

    censoringTimes <- generateEventForSubject(
      baselineSurvival = censoringPlpModel$model$baselineSurvival,
      linearPredictor = censoringLinearPredictor
    )

    generateNewExposureTable(
      connectionDetails = connectionDetails,
      exposureDatabaseSchema = resultDatabaseSchema,
      exposureTable = "sampled_cohorts",
      resultDatabaseSchema = resultDatabaseSchema,
      resultTable = simulatedExposureTable,
      eventTimes = censoringTimes
    )
  }

  cleanupExposureTable(
    connectionDetails = connectionDetails,
    resultDatabaseSchema = resultDatabaseSchema,
    resultTable = simulatedExposureTable,
    cleanup = max
  )

  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails))
  n <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      SELECT COUNT(*) n FROM { resultDatabaseSchema }.{ simulatedExposureTable }
      "
    )
  ) |> dplyr::pull(N)

  message(glue::glue("Simulated population of size {n}"))

}


getSampledCohortsCovariateData <- function(
  connectionDetails,
  cdmDatabaseSchema,
  cohortTable,
  cohortIds,
  cohortDatabaseSchema,
  covariateSettings
) {

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

  if (missing(cohortIds)) {

    sqlQuery1 <- glue::glue(
      "
      SELECT DISTINCT
        cohort_definition_id,
        source_subject_id subject_id,
        cohort_start_date,
        cohort_end_date
      FROM { cohortDatabaseSchema }.{ cohortTable };
      "
    )

    sqlQuery2 <- glue::glue(
      "
      SELECT subject_id, source_subject_id
      FROM { cohortDatabaseSchema }.{ cohortTable };
      "
    )
  } else {

    sqlQuery1 <- glue::glue(
      "
      SELECT DISTINCT
        cohort_definition_id,
        source_subject_id subject_id,
        cohort_start_date,
        cohort_end_date
      FROM { cohortDatabaseSchema }.{ cohortTable }
      WHERE cohort_definition_id IN (
        { glue::glue_collapse(cohortIds, sep = \", \") }
      );
      "
    )

    sqlQuery2 <- glue::glue(
      "
      SELECT subject_id, source_subject_id
      FROM { cohortDatabaseSchema }.{ cohortTable }
      WHERE cohort_definition_id IN (
        { glue::glue_collapse(cohortIds, sep = \", \") }
      );
      "
    )
  }

  distinctPatients <- DatabaseConnector::querySql(connection, sqlQuery1)
  DatabaseConnector::insertTable(
    connection = connection,
    data = distinctPatients,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tableName = "temp_distinct_patients",
  )

  covariateData <- FeatureExtraction::getDbCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "temp_distinct_patients",
    cohortDatabaseSchema = cohortDatabaseSchema,
    covariateSettings = covariateSettings
  )

  covariateData$mapSubjectIds <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery2
  )
  covariateData$newCovariates <- DBI::dbGetQuery(
    covariateData,
    "
    SELECT
      m.subject_id rowId,
      covariateId,
      covariateValue
    FROM covariates c
    JOIN mapSubjectIds m ON c.rowId = m.source_subject_id;
    "
  )

  DBI::dbExecute(covariateData, "DROP TABLE covariates;")
  DBI::dbExecute(
    covariateData,
    "
    ALTER TABLE newCovariates
    RENAME TO covariates;
    "
  )

  DatabaseConnector::dbExecute(connection, "DROP TABLE temp_distinct_patients")

  covariateData
}

generateSample <- function(
  connectionDetails,
  size,
  exposureDatabaseSchema,
  exposureTable,
  exposureIds,
  resultDatabaseSchema,
  simulatedCohortTable = "sampled_cohorts"
) {

  connection <- DatabaseConnector::connect(connectionDetails)

  message("Sampling cohorts...")
  sqlQuery <- glue::glue(
    "
    SELECT DISTINCT(subject_id)
    FROM { exposureDatabaseSchema }.{ exposureTable }
    WHERE cohort_definition_id IN (
      { glue::glue_collapse(exposureIds, sep = \", \") }
    );
    "
  )

  subjectIds <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery
  ) |>
    unlist(use.names = FALSE)

  if (size > length(subjectIds)) {
    warning("Size of simulated dataset larger than original population")
  }

  subjectIds <- subjectIds |>
    sample(size = size, replace = TRUE)

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = "temp_sampled_subject_ids",
    data = data.frame(subject_id = subjectIds),
    tempTable = TRUE
  )

  # sqlQuery <- glue::glue(
  #   "
  #   WITH target_cohorts AS (
  #     SELECT *
  #     FROM { exposureDatabaseSchema }.{ exposureTable }
  #     WHERE cohort_definition_id IN (
  #       { glue::glue_collapse(exposureIds, sep = \", \") }
  #     )
  #   )
  #   SELECT *
  #   FROM target_cohorts
  #   WHERE subject_id IN (
  #     SELECT subject_id FROM { resultDatabaseSchema }.temp_sampled_subject_ids
  #   )
  #   "
  # )

  sqlQuery <- glue::glue(
    "
     SELECT *
     FROM { exposureDatabaseSchema }.{ exposureTable }
     WHERE exposure_cohort_definition_id IN (
       { glue::glue_collapse(exposureIds, sep = \", \") }
     )
    "
  )

  # Selects only the first drug exposure for each patient
  # If a patient is present in two or more exposure cohorts
  # Only the earliest is selected
  # sqlQuery <- glue::glue(
  #   "
  #   SELECT
  #     cohort_definition_id,
  #     subject_id,
  #     cohort_start_date,
  #     cohort_end_date
  #   FROM (
  #     SELECT
  #       *,
  #       ROW_NUMBER() OVER (
  #         PARTITION BY subject_id ORDER BY cohort_start_date ASC
  #       ) as rn
  #     FROM { exposureDatabaseSchema }.{ exposureTable }
  #     WHERE subject_id IN (
  #       SELECT subject_id FROM { resultDatabaseSchema }.temp_sampled_subject_ids
  #     )
  #     AND cohort_definition_id IN (
  #       { glue::glue_collapse(exposureIds, sep = \", \") }
  #     )
  #   ) sub
  #   WHERE rn = 1
  #   ORDER BY cohort_definition_id, subject_id, cohort_start_date
  #   "
  # )

  sampledCohorts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery
  ) |>
    dplyr::sample_n(
      size = size,
      replace = TRUE
    )

  result <- sampledCohorts |>
    dplyr::rename_with(convertToCamelCase) |>
    tidyr::nest(.by = exposureCohortDefinitionId) |>
    dplyr::mutate(
      test = purrr::map(
        .x = data,
        .f = \(x) {
          x |>
            dplyr::rename("sourceSubjectId" = "subjectId") |>
            dplyr::arrange(sourceSubjectId) |>
            dplyr::mutate(subjectId = 1:dplyr::n())
        }
      )
    ) |>
    dplyr::select(-data) |>
    tidyr::unnest(cols = test) |>
    dplyr::arrange(exposureCohortDefinitionId, subjectId, sourceSubjectId) |>
    dplyr::select(-cohortDefinitionId) |>
    dplyr::rename("cohortDefinitionId" = "exposureCohortDefinitionId") |>
    dplyr::relocate(subjectId, .before = sourceSubjectId)

  message("Storing sampled cohorts")
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = simulatedCohortTable,
    data = result |> dplyr::rename_with(convertToSnakeCase),
    tempTable = FALSE,
    dropTableIfExists = TRUE
  )

  DatabaseConnector::disconnect(connection)

}

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

  dplyr::as_tibble(outDT) |>
    dplyr::mutate(outcomeId = plpModel$modelDesign$outcomeId)
}

#' Populate new exposure table
#' 
#' Function that creates and populates the simulated exposure table inside the
#' database
#' 
#' @param connectionDetails The connection details for connecting to the databaseDetails
#' @param exposureDatabaseSchema The schema where the exposure table is located
#' @param exposureTable The exposure table. Should contain columns
#'   `COHORT_DEFINITION_ID`, `SUBJECT_ID`, `COHORT_START_DATE` and
#'   `COHORT_END_DATE`. Column `COHORT_DEFINITION_ID` should contain the
#'   exposure cohort definition ids.
#' @param resultDatabaseSchema The database schema where the new exposure table
#'   will be stored
#' @param resultTable The table with the new exposures
#' @param eventTimes A dataframe with the simulated event times

generateNewExposureTable <- function(
  connectionDetails,
  exposureDatabaseSchema,
  exposureTable,
  resultDatabaseSchema,
  resultTable,
  eventTimes
) {

  connection <- DatabaseConnector::connect(connectionDetails)

  tableNames <- DatabaseConnector::getTableNames(connection)
  if (!resultTable %in% tableNames) {
    createSql <- glue::glue(
      "
       CREATE TABLE { resultDatabaseSchema }.{ resultTable } (
         COHORT_DEFINITION_ID          INTEGER,
         OUTCOME_COHORT_DEFINITION_ID INTEGER,
         SUBJECT_ID                    BIGINT,
         SOURCE_SUBJECT_ID             BIGINT,
         COHORT_START_DATE             DATE,
         COHORT_END_DATE               DATE
       );
       "
    )
    DatabaseConnector::executeSql(connection, createSql)
  }

  patients <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      SELECT * FROM { exposureDatabaseSchema }.{ exposureTable }
      "
    )
  )

  result <-
    patients |>
    dplyr::rename_with(convertToCamelCase, .cols = dplyr::everything()) |>
    dplyr::inner_join(eventTimes, by = c("subjectId" = "rowId")) |>
    dplyr::select(
      c(
        "cohortDefinitionId",
        "outcomeId",
        "subjectId",
        "sourceSubjectId",
        "cohortStartDate",
        "time"
      )
    ) |>
    dplyr::mutate(
      cohortEndDate = cohortStartDate + lubridate::days(time)
    ) |>
    dplyr::select(-"time") |>
    dplyr::rename("outcomeCohortDefinitionId" = "outcomeId") |>
    dplyr::arrange(cohortDefinitionId) |>
    dplyr::rename_with(convertToSnakeCase)

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = resultTable,
    data = result,
    createTable = FALSE,
    dropTableIfExists = FALSE
  )

  message("Added lines to table with simulated exposures")
}

generateNewOutcomeTable <- function(
  connectionDetails,
  exposureDatabaseSchema,
  exposureTable,
  resultDatabaseSchema,
  resultTable,
  eventTimes
) {

  connection <- DatabaseConnector::connect(connectionDetails)

  tableNames <- DatabaseConnector::getTableNames(connection)
  if (!resultTable %in% tableNames) {
    createSql <- glue::glue(
      "
       CREATE TABLE { resultDatabaseSchema }.{ resultTable } (
         COHORT_DEFINITION_ID          INTEGER,
         SUBJECT_ID                    BIGINT,
         SOURCE_SUBJECT_ID             BIGINT,
         COHORT_START_DATE             DATE,
         COHORT_END_DATE               DATE
       );
       "
    )
    DatabaseConnector::executeSql(connection, createSql)
  }

  patients <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      SELECT * FROM { exposureDatabaseSchema }.{ exposureTable }
      "
    )
  ) |>
    dplyr::as_tibble()

  result <- patients |>
    dplyr::rename_with(convertToCamelCase, .cols = dplyr::everything()) |>
    dplyr::inner_join(eventTimes, by = c("subjectId" = "rowId")) |>
    dplyr::filter(event) |>
    dplyr::select(
      c(
        "outcomeId",
        "subjectId",
        "sourceSubjectId",
        "cohortStartDate",
        "time"
      )
    ) |>
    dplyr::mutate(
      cohortEndDate = cohortStartDate + lubridate::days(time)
    ) |>
    dplyr::select(-"time") |>
    dplyr::mutate(cohortStartDate = cohortEndDate) |>
    dplyr::rename("cohortDefinitionId" = "outcomeId") |>
    dplyr::relocate(cohortDefinitionId) |>
    dplyr::arrange(cohortDefinitionId) |>
    dplyr::rename_with(convertToSnakeCase)

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = resultTable,
    data = result,
    createTable = FALSE,
    dropTableIfExists = FALSE
  )

  message("Added lines to table with simulated outcomes")
}



generateCensoringTable <- function(
  connectionDetails,
  censoringTime,
  exposureDatabaseSchema,
  exposureTable,
  outcomeDatabaseSchema,
  outcomeTable,
  outcomeIds,
  resultDatabaseSchema,
  resultTable
) {

  connection <- DatabaseConnector::connect(connectionDetails)
  buildSelectSql <- function(outcomeId) {
    glue::glue(
      "
      WITH censor_base AS (
        SELECT
          exposure_cohort_definition_id,
          subject_id,
          cohort_start_date AS exp_start,
          cohort_end_date,
          CASE
            WHEN cohort_end_date < DATE_ADD(cohort_start_date, INTERVAL '{censoringTime} days')
              THEN cohort_end_date
            ELSE DATE_ADD(cohort_start_date, INTERVAL '{censoringTime} days')
          END AS censor_date
        FROM {exposureDatabaseSchema}.{exposureTable}
      )
      SELECT
        {outcomeId} AS cohort_definition_id,
        exposure_cohort_definition_id,
        cb.subject_id,
        cb.censor_date        AS cohort_start_date,
        cb.censor_date        AS cohort_end_date
      FROM censor_base cb
      WHERE NOT EXISTS (
        SELECT 1
        FROM {outcomeDatabaseSchema}.{outcomeTable} o
        WHERE o.subject_id           = cb.subject_id
          AND o.cohort_definition_id = {outcomeId}
          AND o.cohort_start_date   >= cb.exp_start
          AND o.cohort_start_date   <= cb.censor_date
      )
      "
    )
  }

  tableNames <- DatabaseConnector::getTableNames(connection)
  if (!resultTable %in% tableNames) {
    createSql <- glue::glue(
      "
       CREATE TABLE { resultDatabaseSchema }.{ resultTable } (
         cohort_definition_id          INTEGER,
         exposure_cohort_definition_id INTEGER,
         subject_id                    BIGINT,
         cohort_start_date             DATE,
         cohort_end_date               DATE
       );
       "
    )
    DatabaseConnector::executeSql(connection, createSql)
  }

  for (i in seq_along(outcomeIds)) {
    outcomeId <- outcomeIds[i]
    selectSql <- buildSelectSql(outcomeId)

    insertSql <- glue::glue(
      "INSERT INTO {resultDatabaseSchema}.{resultTable}\n{selectSql}"
    )
    DatabaseConnector::executeSql(connection, insertSql)
    message(sprintf(
      "Appended outcomeId = %s to %s.%s",
      outcomeId, resultDatabaseSchema, resultTable
    ))
  }
  message("All outcomeIds processed.")
  DatabaseConnector::disconnect(connection)
}


cleanupExposureTable <- function(
  connectionDetails,
  resultDatabaseSchema,
  resultTable,
  cleanup = "max"
) {

  message("Cleaning up table ", resultTable)
  connection <- DatabaseConnector::connect(connectionDetails)
  cleanupFunction <- match.fun(cleanup)

  exposures <- DatabaseConnector::querySql(
    connection = connection,
    glue::glue(
      "
      SELECT * FROM { resultDatabaseSchema }.{ resultTable };
      "
    )
  )

  exposuresDT <- as.data.table(exposures)

  result <- exposuresDT[
    , .(COHORT_END_DATE = cleanupFunction(COHORT_END_DATE)),
    by = .(COHORT_DEFINITION_ID, SUBJECT_ID, SOURCE_SUBJECT_ID, COHORT_START_DATE)
  ]

  DatabaseConnector::insertTable(
    connection = connection,
    data = result,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tableName = resultTable
  )

  DatabaseConnector::disconnect(connection)

  message("Done")
}
