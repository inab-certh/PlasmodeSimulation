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

  # Selects only the first drug exposure for each patient
  # If a patient is present in two or more exposure cohorts
  # Only the earliest is selected
  sqlQuery <- glue::glue(
    "
    SELECT
      cohort_definition_id,
      subject_id,
      cohort_start_date,
      cohort_end_date
    FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER (
          PARTITION BY subject_id ORDER BY cohort_start_date ASC
        ) as rn
      FROM { exposureDatabaseSchema }.{ exposureTable }
      WHERE subject_id IN (
        SELECT subject_id FROM { resultDatabaseSchema }.temp_sampled_subject_ids
      )
      AND cohort_definition_id IN (
        { glue::glue_collapse(exposureIds, sep = \", \") }
      )
    ) sub
    WHERE rn = 1
    ORDER BY cohort_definition_id, subject_id, cohort_start_date
    "
  )

  sampledCohorts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = simulatedCohortTable,
    data = sampledCohorts,
    tempTable = FALSE,
    dropTableIfExists = TRUE
  )

  message("Sampled cohorts stored in table sampled_cohorts")

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
    by = .(COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE)
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
