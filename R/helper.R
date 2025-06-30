addSettings <- function(
  settings,
  flagSame,
  settingsName,
  settingsValue
) {
  if (flagSame) {
    purrr::map(
      settings,
      ~ append(.x, purrr::set_names(list(settingsValue), settingsName))
    )
  } else {
    purrr::map2(
      settings,
      settingsValue,
      ~ append(.x, purrr::set_names(list(.y), settingsName))
    )
  }
}


replaceTargetTableWithExposure <- function(
  connectionDetails,
  cohortDatabaseSchema,
  cohortTable,
  resultDatabaseSchema,
  resultTable
) {

  connection <- DatabaseConnector::connect(connectionDetails)
  message("Creating new exposure table...")
  sqlQuery <- glue::glue(
    "
    DROP TABLE IF EXISTS { resultDatabaseSchema }.{ resultTable };
    CREATE TABLE { resultDatabaseSchema }.{ resultTable } AS
    SELECT
      exposure_cohort_definition_id AS cohort_definition_id,
      subject_id,
      cohort_start_date,
      cohort_end_date
    FROM { cohortDatabaseSchema }.{ cohortTable };
    "
  )

  DatabaseConnector::executeSql(connection, sqlQuery)

  warning("Dropping combined target table not implemented yet")

  # sqlQuery <- glue::glue(
  #   "
  #   DROP TABLE IF EXISTS { cohortDatabaseSchema }.{ cohortTable };
  #   "
  # )
  # DatabaseConnector::executeSql(connection, sqlQuery)

  DatabaseConnector::disconnect(connection)

}
