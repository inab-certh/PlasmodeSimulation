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

  message("Generated table with simulated exposures")
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

  message("Generated table with simulated exposures")
}
