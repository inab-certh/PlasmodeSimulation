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
