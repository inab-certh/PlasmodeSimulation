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
  andromeda,
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

  message("Limiting table: ", cohortTable)
  knownTables <- system.file(
    "csv", "table_start_dates.csv",
    package = "PlasmodeSimulation"
  ) |>
    readr::read_csv(show_col_types = FALSE)

  if (
    any(
      missing(cohortTableStartDateField),
      missing(cohortTableEndDateField),
      missing(cohortTableRowIdField)
    )
  ) {
    if (missing(cohortTable)) {
      stop("Need to define limitTable")
    } else {
      if (cohortTable %in% c(knownTables$table, "person")) {
        tableColumns <- knownTables |>
          dplyr::filter(table == cohortTable)
        cohortTableStartDateField <- tableColumns$start_date
        cohortTableEndDateField <- tableColumns$end_date
        cohortTableRowIdField <- "person_id"
      } else {
        stop("Need to define limitTable, its date fields and rowId field")
      }
    }
  }

  if (cohortTable == "person") {
    sql <- glue::glue(
      "
      SELECT c.*
      FROM { limitDatabaseSchema }.{ limitTable } l
      JOIN { cohortDatabaseSchema }.{ cohortTable } c
        ON c.person_id = l.{ limitTableRowIdField }
      ;
      "
    ) |>
      SqlRender::translate(targetDialect = connection@dbms)

    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = andromeda,
      andromedaTableName = cohortTable
    )

    return(invisible(TRUE))
  }

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


  dateSql <- if (cohortTableStartDateField != cohortTableEndDateField) {
    glue::glue(
      "
      CASE
        WHEN c.{ cohortTableStartDateField } < l.{ limitTableStartDateField }
          THEN l.{ limitTableStartDateField }
          ELSE c.{ cohortTableStartDateField }
      END AS { cohortTableStartDateField },
      CASE
        WHEN c.{ cohortTableEndDateField } > l.{ limitTableEndDateField }
          THEN l.{ limitTableEndDateField }
          ELSE c.{ cohortTableEndDateField }
      END AS { cohortTableEndDateField }
      "
    )
  } else {
    glue::glue("c.{ cohortTableStartDateField } AS { cohortTableStartDateField }")
  }

  whereSql <- if (cohortTableStartDateField != cohortTableEndDateField) {
    glue::glue(
      "
      NOT (
        c.{ cohortTableEndDateField } < l.{ limitTableStartDateField } OR
        c.{ cohortTableStartDateField } > l.{ limitTableEndDateField }
      )
      "
    )
  } else {
    glue::glue(
      "
      c.{ cohortTableStartDateField } >= l.{ limitTableStartDateField } AND
      c.{ cohortTableStartDateField } <= l.{ limitTableEndDateField }
      "
    )
  }

  sql <- glue::glue(
    "
    SELECT
      { retainedColsSql },
      { dateSql }
    FROM { cohortDatabaseSchema }.{ cohortTable } c
    JOIN { limitDatabaseSchema }.{ limitTable } l
      ON c.{ cohortTableRowIdField } = l.{ limitTableRowIdField }
    WHERE { whereSql }
    ;
    "
  ) |>
    SqlRender::translate(targetDialect = connection@dbms)


  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = sql,
    andromeda = andromeda,
    andromedaTableName = cohortTable
  )
  invisible(TRUE)

}


generateLagsMatrix <- function(
  andromeda,
  maxLag = 1,
  lagIdMultiplier = 1e3,
  allTimeIds = NULL
) {

  message("Generating sparse matrix...")
  message(glue::glue("Using maxLag: { maxLag }"))

  createLaggedCovariatesWithMapping(
    andromeda = andromeda,
    maxLag =  maxLag,
    lagIdMultiplier = lagIdMultiplier
  ) |>
    dplyr::collect() |>
    buildSparseTemporal(allTimeIds = allTimeIds)
}

getMaxPeriod <- function(
  andromeda,
  table,
  startDate,
  endDate,
  periodLength = 1
) {

  # Calculate the maximum duration in days from the specified table and columns
  maxDurationDays <- andromeda[[table]] |>
    dplyr::collect() |>
    dplyr::mutate(
      periodDuration = lubridate::as_date(.data[[endDate]]) -
        lubridate::as_date(.data[[startDate]])
    ) |>
    dplyr::summarise(maxDuration = max(periodDuration, na.rm = TRUE)) |>
    dplyr::pull(maxDuration)

  # Calculate the number of periods
  numPeriods <- ceiling(as.numeric(maxDurationDays) / periodLength)

  # Construct and return the final string
  message(
    glue::glue("Time difference of { numPeriods } periods ({ periodLength } days)")
  )
}

#' Combine multiple sparse feature matrices into a single matrix with a unified row space.
#' This version ensures the 'timeId' column is fully populated for all rows.
#'
#' @param matrixList A named list of outputs from generateLagsMatrix.
#'                   Example: list(covariates = cov_matrix, outcomes = out_matrix, exposures = exp_matrix).
#'                   Each element must contain $sparseMatrix, $rowMapping, and $colMapping.
#' @return A list containing the combined $sparseMatrix, and the new master $rowMapping and $colMapping.
combineFeatureMatrices <- function(matrixList) {

  message("Combining feature matrices...")

  # --- 1. Establish the Universal Row Space (Master Row Mapping) ---
  message("Step 1: Establishing universal row space...")
  allRowMappings <- purrr::map(matrixList, "rowMapping")

  allRowIds <- allRowMappings |>
    purrr::map("rowId") |>
    unlist() |>
    unique() |>
    sort()

  allTimeIds <- allRowMappings |>
    purrr::map("timeId") |>
    unlist() |>
    range() |>
    (function(r) seq.int(r[1], r[2]))()

  masterRowMapping <- tidyr::expand_grid(rowId = allRowIds, timeId = allTimeIds) |>
    dplyr::mutate(
      rowKey = paste(rowId, timeId, sep = "_"),
      matrixRow = dplyr::row_number()
    ) |>
    dplyr::select(matrixRow, rowKey, rowId, timeId)

  masterRowIndexMap <- stats::setNames(masterRowMapping$matrixRow, masterRowMapping$rowKey)

  # --- 2. Establish the Universal Feature Space (Master Column Mapping) ---
  message("Step 2: Establishing universal feature space...")
  allColMappings <- purrr::map_dfr(matrixList, "colMapping", .id = "source")

  masterColMapping <- allColMappings |>
    dplyr::distinct(covariateId, .keep_all = TRUE) |>
    dplyr::arrange(source, baseId, lag) |>
    dplyr::mutate(matrixCol = dplyr::row_number())

  masterColIndexMap <- stats::setNames(
    masterColMapping$matrixCol,
    ifelse(is.na(masterColMapping$covariateId), "timeId", as.character(masterColMapping$covariateId))
  )

  # --- 3. Extract, Re-map, and Combine Non-Zero Triplets ---
  message("Step 3: Remapping non-zero entries from all matrices...")
  
  # A. Get triplets for all FEATURES, explicitly excluding the 'timeId' column for now.
  allFeatureTriplets <- purrr::map_dfr(matrixList, function(component) {
    mat <- component$sparseMatrix
    if (nrow(mat) == 0) return(NULL)

    summaryDf <- as.data.frame(Matrix::summary(mat))
    
    summaryDf |>
      dplyr::transmute(
        rowKey = dimnames(mat)[[1]][i],
        colKey = dimnames(mat)[[2]][j],
        value = x
      ) |>
      # KEY CHANGE: Exclude the timeId column from this part of the process
      dplyr::filter(colKey != "timeId")
  })

  # B. KEY CHANGE: Create a complete set of triplets for the 'timeId' column.
  # This guarantees a value for every single row in the final matrix.
  timeIdTriplets <- masterRowMapping |>
    dplyr::transmute(
      rowKey = rowKey,
      colKey = "timeId",
      value = as.numeric(timeId)
    )

  # C. Combine the feature triplets and the complete timeId triplets
  allTriplets <- dplyr::bind_rows(allFeatureTriplets, timeIdTriplets) |>
    dplyr::filter(rowKey %in% names(masterRowIndexMap), colKey %in% names(masterColIndexMap))


  # --- 4. Build the Final Sparse Matrix ---
  message("Step 4: Assembling final sparse matrix...")
  finalMatrix <- Matrix::sparseMatrix(
    i = masterRowIndexMap[allTriplets$rowKey],
    j = masterColIndexMap[allTriplets$colKey],
    x = allTriplets$value,
    dims = c(nrow(masterRowMapping), nrow(masterColMapping)),
    dimnames = list(masterRowMapping$rowKey, names(masterColIndexMap))
  )

  message("Done.")

  list(
    sparseMatrix = finalMatrix,
    rowMapping = masterRowMapping,
    colMapping = masterColMapping
  )
}

connectToAndromeda <- function(
  filePath,
  readOnly = TRUE,
  keepExtract = FALSE,
  verbose = TRUE
) {
  if (!file.exists(filePath)) stop("File not found: ", filePath)
  if (!requireNamespace("DatabaseConnector", quietly = TRUE)) {
    stop("Package 'DatabaseConnector' is required.")
  }

  extractDir <- file.path(
    tempdir(),
    paste0(
      "andromeda_",
      format(Sys.time(), "%Y%m%d_%H%M%S_"),
      sample(1e6, 1)
    )
  )
  dir.create(extractDir, recursive = TRUE, showWarnings = FALSE)

  # If something fails before we return the connection, clean up
  on.exit({
    if (!exists("conn", inherits = FALSE)) {
      unlink(extractDir, recursive = TRUE, force = TRUE)
    }
  }, add = TRUE)

  utils::unzip(filePath, exdir = extractDir)

  # Locate the DuckDB file (pick the largest if multiple are present)
  dbFiles <- list.files(
    extractDir,
    pattern = "\\.duckdb$",
    recursive = TRUE,
    full.names = TRUE
  )
  if (length(dbFiles) == 0) {
    stop("No .duckdb file found inside: ", filePath,
         "\nContents extracted to: ", extractDir)
  }
  if (length(dbFiles) > 1 && verbose) {
    message("Multiple .duckdb files found; choosing the largest one.")
  }
  dbPath <- dbFiles[which.max(file.info(dbFiles)$size)]

  if (verbose) {
    message("Connecting to DuckDB at: ", dbPath)
  }

  extra <- if (isTRUE(readOnly)) "read_only=TRUE" else NULL
  cd <- DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = dbPath,
    extraSettings = extra
  )

  connection <- DatabaseConnector::connect(cd)

  # Return a small handle with a cleanup-aware disconnect()
  disconnect <- function() {
    try(DatabaseConnector::disconnect(connection), silent = TRUE)
    if (!keepExtract) unlink(extractDir, recursive = TRUE, force = TRUE)
  }

  structure(
    list(
      connection = connection,
      dbPath = dbPath,
      extractDir = extractDir,
      disconnect = disconnect
    ),
    class = "AndromedaConnection"
  )
}

extractConceptIds <- function(model) {

  model$model$betas |>
    dplyr::filter(covariateId != "(Intercept)") |>
    dplyr::select(covariateId) |>
    dplyr::collect() |>
    dplyr::mutate(covariateId = as.numeric(covariateId)) |>
    dplyr::left_join(
      model$model$covariateRef |> dplyr::collect(),
      by = "covariateId"
    ) |>
    dplyr::pull("conceptId") |>
    unique()
}

truncateCohortTable <- function(
  andromeda,
  tableName,
  stepCohortTableName = "step_cohorts"
  ) {

  andromeda[[tableName]] <- andromeda[[tableName]] |>
    dplyr::left_join(
      andromeda[[stepCohortTableName]] |>
        dplyr::select(
          dplyr::all_of(
            c(
              "subject_id",
              "cohort_start_date",
              "cohort_end_date"
            )
          )
        ) |>
        dplyr::rename(
          c(
            "target_start_date" = "cohort_start_date",
            "target_end_date" = "cohort_end_date",
            )
        ),
      by = "subject_id"
    ) |>
    dplyr::filter(
      .data[["cohort_start_date"]] < .data[["target_start_date"]]
    ) |>
    dplyr::mutate(
      cohort_end_date = ifelse(
        .data[["cohort_end_date"]] >= .data[["target_start_date"]],
        .data[["target_start_date"]] - lubridate::days(1),
        .data[["cohort_end_date"]]
      )
    ) |>
    dplyr::select(
      dplyr::all_of(
        c(
          "cohort_definition_id",
          "subject_id",
          "cohort_start_date",
          "cohort_end_date"
        )
      )
    ) |>
    dplyr::arrange(
      c(.data[["cohort_definition_id"]], .data[["subject_id"]])
    )
} 

expandModelMatrix <- function(
  andromeda,
  expandFromTable = "person",
  rowIdField = "person_id",
  modelMatrix,
  timeIds
) {

  missingRowIds <- andromeda[[expandFromTable]] |>
    dplyr::distinct(.data[[rowIdField]]) |>
    dplyr::pull() |>
    setdiff(
      modelMatrix$rowMapping |>
        dplyr::distinct(rowId) |>
        dplyr::pull()
    ) |>
    sort()

  rowsToAppend <- expand.grid(rowId = missingRowIds, timeId = timeIds) |>
    dplyr::mutate(
      rowKey = paste(.data[["rowId"]], .data[["timeId"]], sep = "_"),
      matrixRow = dplyr::row_number() + nrow(modelMatrix$sparseMatrix)
    ) |>
    dplyr::relocate(c("matrixRow", "rowKey"))

  modelMatrix$rowMapping <- modelMatrix$rowMapping |>
    dplyr::bind_rows(rowsToAppend) |>
    dplyr::as_tibble()

  zeroRows <- Matrix::sparseMatrix(
    i = integer(0), j = integer(0),
    dims = c(nrow(rowsToAppend), ncol(modelMatrix$sparseMatrix))
  )

  rownames(zeroRows) <- rowsToAppend |>
    dplyr::pull(.data[["rowKey"]])

  modelMatrix$sparseMatrix <- modelMatrix$sparseMatrix |>
    rbind(zeroRows)

  modelMatrix
}
