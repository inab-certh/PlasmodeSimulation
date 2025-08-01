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


convertToCamelCase <- function(x) {
  vapply(x, function(one) {
    one |>
      stringr::str_to_lower() |>
      stringr::str_split_fixed("_", Inf) |>
      stringr::str_to_title() |>
      paste(collapse = "") |>
      stringr::str_replace_all("\\b.", ~ stringr::str_to_lower(.x))
  }, character(1))
}

convertToSnakeCase <- function(x, capitalize = TRUE) {

  result <- x |>
    stringr::str_replace_all("(?<=[a-z0-9])([A-Z])", "_\\1")  |>
    stringr::str_to_lower()

  if (capitalize) {
    result |>
      stringr::str_to_upper()
  } else {
    result
  }
}


createDirIfNotExists <- function(dir) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
    message(glue::glue("Created directory { dir }"))
  }
}


findFile <- function(fileName, dirs) {
  lookFor <- file.path(dirs, fileName)
  found <- file.exists(lookFor)
  lookFor[found][1]

}


dropTableIfExists <- function(
  connectionDetails,
  resultDatabaseSchema,
  tableName
) {

  connection <- suppressMessages(DatabaseConnector::connect(connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection))

  tableNames <- DatabaseConnector::getTableNames(connection)

  if (tableName %in% tableNames) {
    DatabaseConnector::executeSql(
      connection = connection,
      sql = glue::glue(
        "DROP TABLE { resultDatabaseSchema }.{ tableName };"
      )
    )
    message("Dropped existing ", tableName)
  }
}

limitTable <- function(
  connection,
  andromeda,
  fromDatabaseSchema,
  cohortObservationPeriodTable,
  resultDatabaseSchema,
  targetTable,
  targetTableName
) {

  if (missing(targetTableName)) targetTableName <- targetTable

  sqlQuery  <- writeLimitQueryForTable(
    fromDatabaseSchema = fromDatabaseSchema,
    resultDatabaseSchema = resultDatabaseSchema,
    cohortObservationPeriodTable = cohortObservationPeriodTable,
    tableName = targetTable
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = sqlQuery,
    andromeda = andromeda,
    andromedaTableName = targetTableName
  )
}

extractTable <- function(
  connection,
  andromeda,
  fromDatabaseSchema,
  targetTable,
  targetTableName
) {

  if (missing(targetTableName)) targetTableName <- targetTable

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = glue::glue(
      "
      SELECT *
      FROM { fromDatabaseSchema }.{ targetTable };
      "
    ),
    andromeda = andromeda,
    andromedaTableName = targetTableName
  )
}

writeLimitQueryForTable <- function(
  fromDatabaseSchema,
  resultDatabaseSchema,
  cohortObservationPeriodTable,
  tableName
) {

  startDateColumns <- system.file(
    "csv", "table_start_dates.csv",
    package = "PlasmodeSimulation"
  ) |>
    readr::read_csv(show_col_types = FALSE)

  if (tableName %in% startDateColumns$table) {
    startDateName <- startDateColumns |>
      dplyr::filter(table == !!tableName) |>
      dplyr::pull("start_date")

    result <- glue::glue(
      "
      SELECT t.*
      FROM { fromDatabaseSchema }.{ tableName } t
      JOIN { resultDatabaseSchema }.{ cohortObservationPeriodTable } c
        ON c.subject_id = t.person_id
      WHERE t.{ startDateName } <= c.cohort_end_date;
      "
    )
  } else {
    result <- glue::glue(
      "
      SELECT t.*
      FROM { fromDatabaseSchema }.{ tableName } t
      JOIN { resultDatabaseSchema }.{ cohortObservationPeriodTable } c
        ON c.subject_id = t.person_id;
      "
    )
  }

}


limitCohortTable <- function(
  connection,
  connectionDetails,
  cohortDatabaseSchema,
  cohortTable,
  limitDatabaseSchema,
  limitTable,
  cohortTableStartDateField,
  cohortTableEndDateField,
  cohortTableRowIdField,
  limitTableStartDateField,
  limitTableEndDateField,
  limitTableRowIdField
) {

  if (!missing(connectionDetails)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  limitedTable <- paste0(cohortTable, "_limited")
  limitedTableFull <- glue::glue("{ cohortDatabaseSchema }.{ limitedTable }")

  dropTableIfExists(
    connectionDetails = connectionDetails,
    resultDatabaseSchema = cohortDatabaseSchema,
    tableName = limitedTable
  )

  fields <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      SELECT *
      FROM { cohortDatabaseSchema }.{ cohortTable }
      LIMIT 5
      ;
      "
    )
  ) |>
    dplyr::rename_with(tolower) |>
    names()
  
  retainedCols <- fields[
    !fields %in% c(cohortTableStartDateField, cohortTableEndDateField)
  ]
  retainedColsSql <- paste(paste0("c.", retainedCols), collapse = ", ")

  sql <- glue::glue(
    "
    CREATE TABLE { limitedTableFull } AS
    SELECT
      { retainedColsSql },
      CASE
        WHEN c.{cohortTableStartDateField} < l.{limitTableStartDateField}
          THEN l.{limitTableStartDateField}
          ELSE c.{cohortTableStartDateField}
      END AS cohort_start_date,
      CASE
        WHEN c.{cohortTableEndDateField} > l.{limitTableEndDateField}
          THEN l.{limitTableEndDateField}
          ELSE c.{cohortTableEndDateField}
      END AS cohort_end_date
    FROM {cohortDatabaseSchema}.{cohortTable} c
    JOIN {limitDatabaseSchema}.{limitTable} l
      ON c.{ cohortTableRowIdField } = l.{ limitTableRowIdField }
    WHERE NOT (
        c.{cohortTableEndDateField} < l.{limitTableStartDateField} OR
        c.{cohortTableStartDateField} > l.{limitTableEndDateField}
    )
    "
  ) |>
    SqlRender::translate(targetDialect = connection@dbms)


  tryCatch({
    DatabaseConnector::executeSql(connection, sql)
    message(
      "Cohort table successfully limited and stored as: ", limitedTableFull
    )
    TRUE
  }, error = function(e) {
    message(
      "Failed to execute SQL: ", e$message
    )
    FALSE
  })
}
