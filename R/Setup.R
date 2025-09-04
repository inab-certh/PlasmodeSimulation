#' Limit a cdm database to a cohort
#'
#' @description Creates a self-contained cdm database based only in the patients
#'     included in a cohort.
#'
#' @param connectionDetails The connection details to the database. Should be an
#'     object of type \code{\link[DatabaseConnector]{createConnectionDetails}}.
#' @param cdmDatabaseSchema The database schema where the cdm database is
#'     stored.
#' @param exposureDatabaseSchema The database schema where table with the
#'     exposure cohorts are stored.
#' @param exposureTable The table where the exposure cohorts are stored.
#' @param outcomeDatabaseSchema The database schema where the table with the
#'     outcome cohorts is stored.
#' @param outcomeTable The table with the outcome cohorts.
#' @param resultDatabaseSchema The database schema where the table with the
#'     combined cohorts will be stored. Needs write permission.
#' @param cohortObservationPeriodTable The table with the new observation
#'     periods.
#' @param includeOutcomePatients Whether to include patients in the outcome
#'   cohorts to the limiting step.
#' @param limitTableNames The database tables which will be limited to the
#'     cohorts.
#' @param transferTableNames The database tables which will be transferred
#'     unchanged to the new database.
#' @param saveDir The directory where the limited cdm database will be stored.
#'
#' @export
limitCdmToCohort <- function(
  connectionDetails,
  cdmDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  outcomeDatabaseSchema,
  outcomeTable,
  resultDatabaseSchema,
  cohortObservationPeriodTable,
  includeOutcomePatients = FALSE,
  limitTableNames = "all",
  transferTableNames = "all",
  saveDir
) {

  message("Generating database limited to cohorts. This might take a while")

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  exposureSubjectIds <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      SELECT DISTINCT subject_id
      FROM { exposureDatabaseSchema }.{ exposureTable }
      ORDER BY subject_id;
      "
    )
  )

  if (includeOutcomePatients) {
    resultSubjectIds <- DatabaseConnector::querySql(
      connection = connection,
      sql = glue::glue(
        "
      SELECT DISTINCT subject_id
      FROM { outcomeDatabaseSchema }.{ outcomeTable }
      ORDER BY subject_id;
      "
      )
    ) |>
      dplyr::full_join(exposureSubjectIds, by = "SUBJECT_ID")

  } else {
    resultSubjectIds <- exposureSubjectIds
  }

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = "temp_subject_ids",
    data = resultSubjectIds,
    tempTable = TRUE
  )

  result <- Andromeda::andromeda()

  message("Limiting tables to cohorts...")

  if (limitTableNames[1] == "all") {
    tableNames <- c(
      "condition_occurrence", "condition_era", "death",
      "device_exposure", "dose_era", "drug_era", "drug_exposure",
      "episode", "measurement", "note", "observation",
      "payer_plan_period", "person", "procedure_occurrence",
      "visit_detail", "visit_occurrence", "observation_period"
    )
  } else {
    tableNames <- limitTableNames
  }

  purrr::walk(
    .x = tableNames,
    .f = limitTable,
    connection = connection,
    andromeda = result,
    fromDatabaseSchema = cdmDatabaseSchema,
    cohortObservationPeriodTable = cohortObservationPeriodTable,
    resultDatabaseSchema = resultDatabaseSchema,
    .progress = TRUE
  )

  message("Transferring tables...")

  if (transferTableNames[1] == "all") {
    tableNames <- c(
      "care_site", "cdm_source", "fact_relationship",
      "location", "note_nlp", "provider", "vocabulary",
      "concept", "concept_ancestor", "concept_class",
      "concept_relationship", "concept_synonym"
    )
  } else {
    tableNames <- transferTableNames
  }

  purrr::walk(
    .x = tableNames,
    .f = extractTable,
    connection = connection,
    andromeda = result,
    fromDatabaseSchema = cdmDatabaseSchema,
    .progress = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = glue::glue(
      "
      SELECT
        ROW_NUMBER() OVER (ORDER BY subject_id, cohort_start_date) AS observation_period_id,
        subject_id AS person_id,
        cohort_start_date AS observation_period_start_date,
        cohort_end_date AS observation_period_end_date,
        44814724 AS period_type_concept_id
      FROM { resultDatabaseSchema }.{ cohortObservationPeriodTable };
      "
    ),
    andromeda = result,
    andromedaTableName = "observation_period"
  )

  extractTable(
    connection = connection,
    andromeda = result,
    fromDatabaseSchema = cdmDatabaseSchema,
    targetTable = cohortObservationPeriodTable,
    targetTableName = "cohort"
  )

  extractTable(
    connection = connection,
    andromeda = result,
    fromDatabaseSchema = exposureDatabaseSchema,
    targetTable = exposureTable,
    targetTableName = "exposure"
  )

  extractTable(
    connection = connection,
    andromeda = result,
    fromDatabaseSchema = outcomeDatabaseSchema,
    targetTable = outcomeTable,
    targetTableName = "outcome"
  )

  createDirIfNotExists(saveDir)
  fileName <- file.path(saveDir, "SimulationDatabase.zip")
  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = fileName
  )

  message("Limited database stored in ", fileName)
}


limitCdmToSample <- function(fromAndromeda, toAndromeda) {
  f1 <- function(fromAndromeda, toAndromeda, tableName) {
    toAndromeda[[tableName]] <-  fromAndromeda[[tableName]] |>
      dplyr::right_join(
        fromAndromeda$sampled_cohorts,
        by = c("person_id" = "source_subject_id")
      ) |>
      dplyr::select(-"person_id") |>
      dplyr::rename("person_id" = "subject_id") |>
      dplyr::collect()
  }

  f2 <- function(fromAndromeda, toAndromeda, tableName) {
    toAndromeda[[tableName]] <-  fromAndromeda[[tableName]] |>
      dplyr::collect()
  }

  fromAndromeda$sampled_cohorts <- toAndromeda$sampled_cohorts |>
    dplyr::select("subject_id", "source_subject_id")

  message("Limiting tables to sample...")

  limitTableNames <- c(
    "condition_occurrence", "condition_era", "death",
    "device_exposure", "dose_era", "drug_era", "drug_exposure",
    "episode", "measurement", "note", "observation", "observation_period",
    "payer_plan_period", "person", "procedure_occurrence",
    "visit_detail", "visit_occurrence"
  )
  tableNames <- intersect(limitTableNames, names(fromAndromeda))

  purrr::walk(
    .x = tableNames,
    .f = f1,
    fromAndromeda = fromAndromeda,
    toAndromeda = toAndromeda,
    .progress = TRUE
  )

  message("Extracting required tables...")
  transferTableNames <- c(
    "care_site", "cdm_source", "fact_relationship",
    "location", "note_nlp", "provider", "vocabulary",
    "concept", "concept_ancestor", "concept_class",
    "concept_relationship", "concept_synonym"
  )
  tableNames <- intersect(transferTableNames, names(fromAndromeda))
  purrr::walk(
    .x = tableNames,
    .f = f2,
    fromAndromeda = fromAndromeda,
    toAndromeda = toAndromeda,
    .progress = TRUE
  )

  toAndromeda$person <- toAndromeda$person |>
    dplyr::mutate(
      year_of_birth = as.integer(.data$year_of_birth),
      month_of_birth = as.integer(.data$month_of_birth),
      day_of_birth = as.integer(.data$day_of_birth)
    )

  toAndromeda$cohort <- fromAndromeda$cohort |>
    dplyr::rename("source_subject_id" = "subject_id") |>
    dplyr::right_join(
      fromAndromeda$sampled_cohorts,
      by = "source_subject_id"
    ) |>
    dplyr::select(-"source_subject_id") |>
    dplyr::collect()

  message("Data limited to sample")
}


generateObservationPeriods <- function(
  connectionDetails,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  cohortTable,
  cohortDefinitionIds,
  resultDatabaseSchema,
  resultTableName,
  anchor = "observation start",
  days = 0
) {

  dropTableIfExists(
    connectionDetails = connectionDetails,
    resultDatabaseSchema = resultDatabaseSchema,
    tableName = resultTableName
  )

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  anchor <- tolower(anchor)
  if (!anchor %in% c("observation start", "observation end")) {
    stop("anchor must be either 'observation start' or 'observation end'")
  }

  if (missing(cohortDefinitionIds) || length(cohortDefinitionIds) == 0) {
    stop("You must provide one or more cohortDefinitionIds.")
  }

  cohortIdList <- glue::glue_collapse(cohortDefinitionIds, sep = ", ")

  resultTableFull <- glue::glue("{ resultDatabaseSchema }.{ resultTableName }")

  sql <- if (anchor == "observation start") {
    glue::glue(
      "
      CREATE TABLE { resultTableFull } AS
      SELECT
        op.person_id AS subject_id,
        op.observation_period_start_date AS cohort_start_date,
        CASE
          WHEN DATEADD(DAY, { days }, op.observation_period_start_date) < op.observation_period_end_date
            THEN DATEADD(DAY, { days }, op.observation_period_start_date)
            ELSE op.observation_period_end_date
        END AS cohort_end_date
      FROM { cdmDatabaseSchema }.observation_period op
      JOIN { cohortDatabaseSchema }.{ cohortTable } c
        ON op.person_id = c.subject_id
      WHERE c.cohort_definition_id IN ({ cohortIdList })
      GROUP BY op.person_id, op.observation_period_start_date, op.observation_period_end_date
      "
    )
  } else {
    glue::glue(
      "
      CREATE TABLE { resultTableFull } AS
      SELECT
        op.person_id AS subject_id,
        CASE
          WHEN DATEADD(DAY, -{ days }, op.observation_period_end_date) > op.observation_period_start_date
            THEN DATEADD(DAY, -{ days }, op.observation_period_end_date)
            ELSE op.observation_period_start_date
        END AS cohort_start_date,
        op.observation_period_end_date AS cohort_end_date
      FROM { cdmDatabaseSchema }.observation_period op
      JOIN { cohortDatabaseSchema }.{ cohortTable } c
        ON op.person_id = c.subject_id
      WHERE c.cohort_definition_id IN ({ cohortIdList })
      GROUP BY op.person_id, op.observation_period_start_date, op.observation_period_end_date
      "
    )
  }

  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)

  tryCatch({
    DatabaseConnector::executeSql(connection, sql)
    TRUE
  }, error = function(e) {
    message("Failed to execute SQL: ", e$message)
    FALSE
  })
}

convertToObservationPeriodTable <- function(
  andromeda,
  tableName,
  rowIdField,
  startDateField,
  endDateField
) {

  andromeda[[tableName]] <- andromeda[[tableName]] |>
    dplyr::collect() |>
    dplyr::rename(
      "person_id" = tidyr::all_of(rowIdField),
      "observation_period_start_date" = tidyr::all_of(startDateField),
      "observation_period_end_date" = tidyr::all_of(endDateField)
    ) |>
    dplyr::arrange(
      .data$person_id,
      .data$observation_period_start_date,
      .data$observation_period_end_date
    ) |>
    dplyr::group_by(.data$person_id) |>
    dplyr::mutate(observation_period_number = dplyr::row_number()) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      observation_period_id = bit64::as.integer64(.data$person_id) * 1e4 +
        .data$observation_period_number
    )
}

combineExposureCohorts <- function(
  andromeda,
  cohortTable,
  cohortDefinitionIds,
  resultTable
) {

  if (cohortDefinitionIds == -1) {
    cohortDefinitionIds <- andromeda[[cohortTable]] |>
      dplyr::select(cohort_definition_id) |>
      dplyr::collect() |>
      dplyr::pull() |>
      unique() |>
      sort()
  }

  andromeda[[resultTable]] <- andromeda[[cohortTable]] |>
    dplyr::filter(cohort_definition_id %in% cohortDefinitionIds) |>
    dplyr::group_by(subject_id) |>
    dplyr::slice_min(order_by = cohort_start_date, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::collect() |>
    dplyr::arrange(cohort_definition_id, subject_id)

}

limitCdmToCohortTable <- function(
  connection,
  connectionDetails,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  cohortTableNames = "all",
  transferTableNames = "all",
  limitDatabaseSchema,
  limitTable,
  limitTableStartDateField,
  limitTableEndDateField,
  limitTableRowIdField,
  andromeda,
  includeLimitTable = FALSE,
  includedLimitTableName,
  ...
) {

  if (includeLimitTable) {
    if (missing(includedLimitTableName)) {
      stop("Need to define includedLimitTableName")
    }

    sql <- glue::glue(
      "
      SELECT *
      FROM { limitDatabaseSchema }.{ limitTable }
      ;
      "
    )

    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = andromeda,
      andromedaTableName = includedLimitTableName
    )

    message(
      glue::glue(
        "
        Transferred limit table { limitDatabaseSchema }.{ limitTable }
        to { includedLimitTableName }
        "
      )
    )
  }

  message("Limiting tables to cohorts...")

  if (cohortTableNames[1] == "all") {
    tableNames <- c(
      "person", "condition_occurrence", "condition_era", "death",
      "device_exposure", "dose_era", "drug_era", "drug_exposure",
      "episode", "measurement", "note", "observation",
      "payer_plan_period", "procedure_occurrence",
      "visit_detail", "visit_occurrence"
    )
  } else {
    tableNames <- cohortTableNames
  }

  xx <- purrr::map(
    .x = tableNames,
    .f = limitCohortTable,
    connection = connection,
    andromeda = andromeda,
    cohortDatabaseSchema = cohortDatabaseSchema,
    limitDatabaseSchema = limitDatabaseSchema,
    limitTableStartDateField = limitTableStartDateField,
    limitTableEndDateField = limitTableEndDateField,
    limitTableRowIdField = limitTableRowIdField,
    limitTable = limitTable,
    .progress = TRUE
  )

  if (transferTableNames[1] == "all") {
    tableNames <- c(
      "care_site", "cdm_source", "fact_relationship",
      "location", "note_nlp", "provider", "vocabulary",
      "concept", "concept_ancestor", "concept_class",
      "concept_relationship", "concept_synonym"
    )
  } else {
    tableNames <- transferTableNames
  }


  .extractTable <- function(
    cdmDatabaseSchema,
    tableName,
    andromeda,
    connection
  ) {

    message(glue::glue("Transferring table: { tableName }"))
    sql <- glue::glue(
      "
      SELECT *
      FROM { cdmDatabaseSchema }.{ tableName }
      ;
      "
    ) |>
      SqlRender::translate(targetDialect = connection@dbms)

    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      andromeda = andromeda,
      andromedaTableName = tableName,
      sql = sql
    )
  }

  purrr::walk(
    .x = tableNames,
    .f = .extractTable,
    connection = connection,
    andromeda = andromeda,
    cdmDatabaseSchema = cdmDatabaseSchema,
    .progress = TRUE
  )

}

stepwiseObservationPeriod <- function(
  andromeda,
  periods = c(1),
  incrementDays
) {
  f <- function(andromeda, k, days) {
    andromeda$observation_period |>
      dplyr::collect() |>
      dplyr::mutate(
        cohort_definition_id = k,
        subject_id = person_id,
        cohort_start_date = lubridate::as_date(observation_period_end_date) -
          lubridate::days(k * days),
        cohort_end_date = observation_period_end_date
      ) |>
      dplyr::filter(cohort_start_date >= observation_period_start_date) |>
      dplyr::select(
        c(
          "cohort_definition_id",
          "subject_id",
          "cohort_start_date",
          "cohort_end_date"
        )
      )
  }
  result <- list()
  for (i in seq_along(periods)) {
    result[[i]] <- f(andromeda, periods[i], incrementDays)
  }
  andromeda$step_cohorts <- result |> dplyr::bind_rows()
  andromeda
}

extractPopulation <- function(
  connection,
  cohortDatabaseSchema,
  cohortTable,
  cohortIds
) {

  DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
     "
     SELECT *
     FROM  { cohortDatabaseSchema }.{ cohortTable }
     WHERE cohort_definition_id IN ({ glue::glue_collapse(cohortIds, sep = ',')})
     ;
     "
    )
  ) |>
    dplyr::as_tibble() |>
    dplyr::rename_with(convertToCamelCase)


} 

createNextPeriodSettings <- function(
  settings,
  periodLength
) {

  nextPeriodSettings <- settings

  if (!is.null(settings$analysisId)) {
    nextPeriodSettings$analysisId <- settings$analysisId + 1
  }

  nextPeriodSettings$temporalStartDays <- 0
  nextPeriodSettings$temporalEndDays <- periodLength - 1

  nextPeriodSettings

}
