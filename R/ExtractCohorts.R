extractCohorts <- function(
  connectionDetails,
  firstExposureOnly = TRUE,
  exposureTable,
  outcomeTable,
  exposureIds,
  outcomeIds,
  cohortTable = "simulated",
  resultDatabaseSchema,
  exposureDatabaseSchema = resultDatabaseSchema,
  outcomeDatabaseSchema = resultDatabaseSchema,
  cdmDatabaseSchema
) {

  connection <- DatabaseConnector::connect(connectionDetails)

  sql_query <- glue::glue("
    CREATE TABLE { resultDatabaseSchema }.{ cohortTable } AS
    SELECT 1 AS cohort_definition_id,
        t.subject_id,
        t.cohort_start_date,
        t.cohort_end_date
    FROM
        { exposureDatabaseSchema }.{ exposureTable } t
    WHERE
        cohort_definition_id IN ( { glue::glue_collapse(exposureIds, sep = \", \") } )
    ;")


  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql_query
  )

  sql_query <- glue::glue("
INSERT INTO { resultDatabaseSchema }.{ cohortTable }
SELECT
 cohort_definition_id + 1000 AS cohort_definition_id,
 subject_id, cohort_start_date, cohort_end_date
FROM
  { outcomeDatabaseSchema }.{ outcomeTable }
WHERE
  cohort_definition_id IN ( { glue::glue_collapse(outcomeIds, sep = \", \") } )
;")

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql_query
  )

}
