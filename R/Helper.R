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
  resultDatabaseSchema,
  subjectIdTable,
  targetTable,
  targetTableName
) {

  if (missing(targetTableName)) targetTableName <- targetTable

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = glue::glue(
      "
      SELECT t.*
      FROM { fromDatabaseSchema }.{ targetTable } t
      JOIN { resultDatabaseSchema }.{ subjectIdTable } s
        ON s.subject_id = t.person_id;
      "
    ),
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
