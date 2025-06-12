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
  outcomeTable,
  exposureIds,
  outcomeIds,
  cohortTable = "combined_target",
  resultDatabaseSchema,
  exposureDatabaseSchema = resultDatabaseSchema,
  outcomeDatabaseSchema = resultDatabaseSchema,
  cdmDatabaseSchema
) {

  connection <- DatabaseConnector::connect(connectionDetails)
  if (connectionDbms == "duckdb") {

    sql_query <- glue::glue(
      "
      DROP TABLE IF EXISTS { resultDatabaseSchema }.{ cohortTable };
      CREATE TABLE { resultDatabaseSchema }.{ cohortTable } AS
      SELECT
          1 AS cohort_definition_id,
          subject_id,
          cohort_start_date,
          cohort_end_date
      FROM (
          SELECT
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
  } else if (connectionDbms == "postgresql") {
    createCohortTableIndex(
      connection = connection,
      cohortDatabaseSchema = exposureDatabaseSchema,
      cohortTable = exposureTable
    )
    sql_query <- glue::glue(
      "
      DROP TABLE IF EXISTS { resultDatabaseSchema }.{ cohortTable };
      CREATE TABLE { resultDatabaseSchema }.{ cohortTable } AS
      SELECT DISTINCT ON (subject_id)
             1           AS cohort_definition_id,
             subject_id,
             cohort_start_date,
             cohort_end_date
      FROM { exposureDatabaseSchema }.{ exposureTable }
      WHERE cohort_definition_id IN (
        { glue::glue_collapse(exposureIds, sep = \", \") }
      )
      ORDER BY subject_id,
               cohort_start_date,
               cohort_end_date;      
    "
    )
  }

  connectionDbms <- DatabaseConnector::dbms(connection)

  if (firstExposureOnly) {

    if (connectionDbms == "duckdb") {
      sql_query <- glue::glue(
        "
        DROP TABLE IF EXISTS { resultDatabaseSchema }.{ cohortTable };
        CREATE TABLE { resultDatabaseSchema }.{ cohortTable } AS
        SELECT
            1 AS cohort_definition_id,
            subject_id,
            cohort_start_date,
            cohort_end_date
        FROM (
            SELECT
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

    } else if (connectionDbms == "postgresql") {
      
      createCohortTableIndex(
        connection = connection,
        cohortDatabaseSchema = exposureDatabaseSchema,
        cohortTable = exposureTable
      )
      sql_query <- glue::glue(
        "
        DROP TABLE IF EXISTS { resultDatabaseSchema }.{ cohortTable };
        CREATE TABLE { resultDatabaseSchema }.{ cohortTable } AS
        SELECT DISTINCT ON (subject_id)
               1 AS cohort_definition_id,
               subject_id,
               cohort_start_date,
               cohort_end_date
        FROM { exposureDatabaseSchema }.{ exposureTable }
        WHERE cohort_definition_id IN (
          { glue::glue_collapse(exposureIds, sep = \", \") }
        )
        ORDER BY subject_id,
                 cohort_start_date,
                 cohort_end_date;      
       "
      )
    }

  }

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql_query
  )

  DatabaseConnector::disconnect(connection)

}
