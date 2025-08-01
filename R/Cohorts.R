#' Extract exposure cohorts
#'
#' @description Extract the exposure cohorts from the exposure table. The
#'     function creates a new table where the combined exposure cohorts will be
#'     stored.
#'
#' @param connectionDetails The connection details to the database. Should be an
#'     object of type \code{\link[DatabaseConnector]{createConnectionDetails}}.
#' @param firstExposureOnly If a patient appears in more than one exposure
#'     cohort, only keep the earliest exposure, censoring at the start of later
#'     exposure.
#' @param cdmDatabaseSchema The database schema where the cdm database is
#'     stored.
#' @param exposureDatabaseSchema The database schema where table with the
#'     exposure cohorts are stored.
#' @param exposureTable The table where the exposure cohorts are stored.
#' @param exposureIds The cohort definition ids of the exposure cohorts.
#' @param resultDatabaseSchema The database schema where the table with the
#'     combined cohorts will be stored. Needs write permission.
#' @param cohortTable The table where the combined exposure cohorts will be
#'     stored.
#'
#' @export
extractCohorts <- function(
  connectionDetails,
  firstExposureOnly = TRUE,
  exposureTable,
  exposureIds,
  cohortTable = "combined_target",
  resultDatabaseSchema,
  exposureDatabaseSchema = resultDatabaseSchema,
  cdmDatabaseSchema
) {

  connection <- DatabaseConnector::connect(connectionDetails)
  connectionDbms <- DatabaseConnector::dbms(connection)

  if (connectionDbms == "duckdb") {

    sql_query <- glue::glue(
      "
      DROP TABLE IF EXISTS { resultDatabaseSchema }.{ cohortTable };
      CREATE TABLE { resultDatabaseSchema }.{ cohortTable } AS
      SELECT
          cohort_definition_id,
          subject_id,
          cohort_start_date,
          cohort_end_date
      FROM (
          SELECT
              cohort_definition_id,
              subject_id,
              cohort_start_date,
              cohort_end_date,
              ROW_NUMBER() OVER (
                  PARTITION BY subject_id
                  ORDER BY cohort_start_date, cohort_end_date
              ) AS rn
          FROM { exposureDatabaseSchema }.{ exposureTable }
          WHERE cohort_definition_id IN (
            { glue::glue_collapse(exposureIds, sep = \", \") }
          )
      ) sub
      WHERE rn = 1;
      "
    )
  } else {

    stop(glue::glue("{ connectionDbms } not supported yet."))

  }

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql_query
  )

  DatabaseConnector::disconnect(connection)

}


#' Generate cohort observation period
#'
#' @description Generates new cohort observation periods for the patients in the
#'     exposure cohorts. At the moment, the new observation period is created by
#'     limiting the actual observation period at the end of the exposure for
#'     each patient.
#'
#' @param connectionDetails The connection details to the database. Should be an
#'     object of type \code{\link[DatabaseConnector]{createConnectionDetails}}.
#' @param cdmDatabaseSchema The database schema where the cdm database is
#'     stored.
#' @param exposureDatabaseSchema The database schema where table with the
#'     exposure cohorts are stored.
#' @param exposureTable The table where the exposure cohorts are stored.
#' @param resultDatabaseSchema The database schema where the table with the new
#'     observation periods will be stored. Needs write permission.
#' @param resultTable The table with the new obsrevation periods.
#' @param maxObservationPeriod The maximum length of the observation period.
#' @param washoutPeriod The minimum number of days before exposure.
#'
#'
#' @export
generateCohortObservationPeriod <- function(
  connectionDetails,
  cdmDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  resultDatabaseSchema,
  resultTable = "cohort_observation_period",
  maxObservationPeriod,
  washoutPeriod
) {

  dropTableIfExists(
    connectionDetails = connectionDetails,
    resultDatabaseSchema = resultDatabaseSchema,
    tableName = resultTable
  )

  sqlQuery <- glue::glue(
    "
    CREATE TABLE { resultTable } AS
    SELECT
      1 AS cohort_definition_id,
      et.subject_id,
      GREATEST(
        et.cohort_start_date - INTERVAL '{maxObservationPeriod} days',
        op.observation_period_start_date + INTERVAL '{washoutPeriod} days'
      ) AS cohort_start_date,
      LEAST(
        et.cohort_end_date,
        op.observation_period_end_date
      ) AS cohort_end_date
    FROM { exposureTable } AS et
    JOIN { cdmDatabaseSchema }.observation_period AS op
      ON et.subject_id = op.person_id
    WHERE
      DATE_DIFF(
        'day',
        op.observation_period_start_date,
        et.cohort_start_date
      ) >= {washoutPeriod}
      AND GREATEST(
            et.cohort_start_date - INTERVAL '{maxObservationPeriod} days',
            op.observation_period_start_date + INTERVAL '{washoutPeriod} days'
          )
          <= LEAST(
               et.cohort_end_date,
               op.observation_period_end_date
             )
    ; 
    "
  )

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  DatabaseConnector::executeSql(connection, sqlQuery)

  message("Populated cohort observation period table")
}


#' Generate model cohorts
#'
#' @description Generate the cohort table for the outcome models
#'
#' @param connectionDetails An R object of type `connectionDetails`.
#' @param cdmDatabaseSchema The name of the database schema where the cdm
#'     database is located.
#' @param resultDatabaseSchema The name of the schema where the model cohort
#'     table will be stored. Need to have write priviliedges.
#' @param resultTable The name of the table that will contain the model cohorts
#' @param washoutPeriod The mininum required continuous observation time prior
#'     to index date for a person to be included in the model cohort.
#' @param maxObservationPeriod The maximum length of the observation period
#'
#'
#' @export
generateModelCohort <- function(
  connectionDetails,
  cdmDatabaseSchema,
  resultDatabaseSchema,
  resultTable,
  washoutPeriod,
  maxObservationPeriod
) {

  message("Generating model cohorts...")

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  if (missing(maxObservationPeriod)) {
    maxObservationPeriod <- 1e5
  }

  targetDialect <- connectionDetails$dbms

  sql <- glue::glue(
    "
    CREATE TABLE {resultDatabaseSchema}.{resultTable} AS
    SELECT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      DATEADD(day, {washoutPeriod}, observation_period_start_date) AS cohort_start_date,
      LEAST(
        DATEADD(day, {washoutPeriod} + {maxObservationPeriod}, observation_period_start_date),
        observation_period_end_date
      ) AS cohort_end_date
    FROM {cdmDatabaseSchema}.observation_period
    WHERE DATEADD(day, {washoutPeriod}, observation_period_start_date) <= observation_period_end_date;
    "
  )

  # Translate the SQL to the target dialect
  translatedSql <- SqlRender::translate(
    sql = sql,
    targetDialect = targetDialect
  )

  dropTableIfExists(
    connectionDetails = connectionDetails,
    resultDatabaseSchema = resultDatabaseSchema,
    tableName = resultTable
  )

  DatabaseConnector::executeSql(connection, translatedSql)

  message("Done")
}

createCohortTableIndex <- function(
  connection,
  cohortTable,
  cohortDatabaseSchema
) {

  message("Indexing cohort table...")
  sql_query <- glue::glue(
    "
    CREATE INDEX IF NOT EXISTS idx_cohort_person_start
        ON { cohortDatabaseSchema }.{ cohortTable }
           (subject_id,
            cohort_start_date,
            cohort_end_date);
    "
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql_query
  )

  message("Done")

}

generatePriorOutcomes <- function(
  cdmDatabaseSchema,
  outcomeDatabaseSchema,
  outcomeTable,
  cohortDatabaseSchema,
  cohortTable,
  startDays = c(-30, -180, -365, -99999),
  startDayLabels = c("short", "medium", "long", "any"),
  ...
) {
  
  if (length(startDays) != length(startDayLabels)) {
    stop("startDays must have same length as startDayLabels.")
  }

  analysisId <- 420
  covariateDataList <- list()

  for (i in seq_along(startDayLabels)) {
    analysisId <- analysisId + 1

    covariateSettings <- FeatureExtraction::createCohortBasedCovariateSettings(
      analysisId = analysisId,
      covariateCohortDatabaseSchema = outcomeDatabaseSchema,
      covariateCohortTable = outcomeTable,
      covariateCohorts = data.frame(
        cohortId = outcomeIds,
        cohortName = paste0("outcome_", outcomeIds)
      ),
      startDay = startDays[i],
      endDay = 0
    )

    covariateDataList[[i]] <- FeatureExtraction::getDbCohortBasedCovariatesData(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = glue::glue("{ cohortDatabaseSchema }.{ cohortTable }"),
      covariateSettings = covariateSettings
    )
  }

  covariateDataListClean <- purrr::map(covariateDataList, function(x) {
    list(
      covariates = dplyr::collect(x$covariates),
      covariateRef = dplyr::collect(x$covariateRef),
      analysisRef = dplyr::collect(x$analysisRef)
    )
  })

  transposed <- transpose(covariateDataListClean)

  mergedData <- purrr::map(transposed, dplyr::bind_rows)

  priorOutcomes <- Andromeda::andromeda()
  priorOutcomes$covariates <- mergedData[[1]]
  priorOutcomes$covariateRef <- mergedData[[2]]
  priorOutcomes$analysisRef <- mergedData[[3]]

  priorOutcomes

}

combineExposureCohorts <- function(
  connection,
  connectionDetails,
  cohortDatabaseSchema,
  cohortTable,
  cohortDefinitionIds,
  ...
) {
    if (missing(connection)) {
    if (missing(connectionDetails)) {
      stop("Either connection or connectionDetails must be provided.")
    } else {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
  }

  if (cohortDefinitionIds == -1) {
    cohortDefinitionIds <- DatabaseConnector::querySql(
      connection = connection,
      sql = glue::glue(
        "
        SELECT DISTINCT cohort_definition_id
        FROM { cohortDatabaseSchema }.{ cohortTable }
        ;
        "
      )
    ) |>
      dplyr::rename_with(convertToSnakeCase, capitalize = FALSE) |>
      dplyr::pull("cohort_definition_id") |>
      sort()
  }

  result <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      WITH cohorts AS (
        SELECT *
        FROM { cohortDatabaseSchema }.{ cohortTable }
        WHERE cohort_definition_id IN (
          {
            glue::glue_collapse(cohortDefinitionIds, sep = ',')
          }
        )
      ),
      ranked_cohorts AS (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY subject_id ORDER BY cohort_start_date ASC
            ) AS rn
          FROM { cohortDatabaseSchema }.{ cohortTable }
      )
      SELECT 1 AS cohort_definition_id,
        subject_id, cohort_start_date, cohort_end_date
      FROM ranked_cohorts
      WHERE rn = 1
      ;
      "
    )
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortDatabaseSchema,
    tableName = "combined_cohort_table",
    data = result,
    ...
  )
}
