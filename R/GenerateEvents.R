runSampleGeneration <- function(
  connectionDetails,
  size,
  cdmDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  exposureIds,
  outcomeIds,
  resultDatabaseSchema,
  sampledCohortTable,
  simulatedOutcomeTable,
  cohortObservationTable,
  covariateSettings,
  modelDir,
  saveDir
) {

  if (missing(simulatedOutcomeTable)) {
    simulatedOutcomeTable <- "simulated_outcomes"
  }

  generateSample(
    connectionDetails = connectionDetails,
    size = size,
    exposureDatabaseSchema = exposureDatabaseSchema,
    exposureTable = exposureTable,
    exposureIds = exposureIds,
    resultDatabaseSchema = resultDatabaseSchema,
    simulatedCohortTable = sampledCohortTable
  )

  covariateData <- getSampledCohortsCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = sampledCohortTable,
    cohortDatabaseSchema = resultDatabaseSchema,
    covariateSettings = covariateSettings
  )

  dropTableIfExists(
    connectionDetails = connectionDetails,
    resultDatabaseSchema = resultDatabaseSchema,
    tableName = simulatedOutcomeTable
  )

  for (i in seq_along(outcomeIds)) {

    message(
      glue::glue(
        "Generating outcome times for outcome: { outcomeIds[i] }"
      )
    )

    thisModel <- readRDS(
      file.path(
        modelDir,
        glue::glue("outcome_{ outcomeIds[i] }"),
        "model.rds"
      )
    )

    linearPredictor <- computePoissonLinearPredictor(
      model = thisModel,
      covariateData = covariateData
    )

    eventTimes <- generatePoissonEventTimes(linearPredictor, 200)

    generateNewOutcomeTable(
      connectionDetails = connectionDetails,
      resultDatabaseSchema = resultDatabaseSchema,
      cohortObservationTable = cohortObservationTable,
      sampledCohortTable = sampledCohortTable,
      resultTable = simulatedOutcomeTable,
      eventTimes = eventTimes,
      outcomeId = outcomeIds[i]
    )

  }

  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails))
  n <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      SELECT COUNT(*) n FROM { resultDatabaseSchema }.{ sampledCohortTable }
      "
    )
  ) |> dplyr::pull(N)

  message(glue::glue("Simulated population of size {n}"))
}

getSampledCohortsCovariateData <- function(
  connectionDetails,
  cdmDatabaseSchema,
  cohortTable,
  cohortIds,
  cohortDatabaseSchema,
  covariateSettings
) {

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

  if (missing(cohortIds)) {

    sqlQuery1 <- glue::glue(
      "
      SELECT DISTINCT
        cohort_definition_id,
        source_subject_id subject_id,
        cohort_start_date,
        cohort_end_date
      FROM { cohortDatabaseSchema }.{ cohortTable };
      "
    )

    sqlQuery2 <- glue::glue(
      "
      SELECT subject_id, source_subject_id
      FROM { cohortDatabaseSchema }.{ cohortTable };
      "
    )
  } else {

    sqlQuery1 <- glue::glue(
      "
      SELECT DISTINCT
        cohort_definition_id,
        source_subject_id subject_id,
        cohort_start_date,
        cohort_end_date
      FROM { cohortDatabaseSchema }.{ cohortTable }
      WHERE cohort_definition_id IN (
        { glue::glue_collapse(cohortIds, sep = \", \") }
      );
      "
    )

    sqlQuery2 <- glue::glue(
      "
      SELECT subject_id, source_subject_id
      FROM { cohortDatabaseSchema }.{ cohortTable }
      WHERE cohort_definition_id IN (
        { glue::glue_collapse(cohortIds, sep = \", \") }
      );
      "
    )
  }

  distinctPatients <- DatabaseConnector::querySql(connection, sqlQuery1)
  DatabaseConnector::insertTable(
    connection = connection,
    data = distinctPatients,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tableName = "temp_distinct_patients",
  )

  covariateData <- FeatureExtraction::getDbCovariateData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "temp_distinct_patients",
    cohortDatabaseSchema = cohortDatabaseSchema,
    covariateSettings = covariateSettings
  )

  covariateData$mapSubjectIds <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery2
  )
  covariateData$newCovariates <- DBI::dbGetQuery(
    covariateData,
    "
    SELECT
      m.subject_id rowId,
      covariateId,
      covariateValue
    FROM covariates c
    JOIN mapSubjectIds m ON c.rowId = m.source_subject_id;
    "
  )

  DBI::dbExecute(covariateData, "DROP TABLE covariates;")
  DBI::dbExecute(
    covariateData,
    "
    ALTER TABLE newCovariates
    RENAME TO covariates;
    "
  )

  DatabaseConnector::dbExecute(connection, "DROP TABLE temp_distinct_patients")

  covariateData
}

generateSample <- function(
  connectionDetails,
  size,
  exposureDatabaseSchema,
  exposureTable,
  exposureIds,
  resultDatabaseSchema,
  simulatedCohortTable = "sampled_cohorts"
) {

  connection <- DatabaseConnector::connect(connectionDetails)

  message("Sampling cohorts...")
  sqlQuery <- glue::glue(
    "
    SELECT DISTINCT(subject_id)
    FROM { exposureDatabaseSchema }.{ exposureTable }
    WHERE cohort_definition_id IN (
      { glue::glue_collapse(exposureIds, sep = \", \") }
    );
    "
  )

  sqlQuery <- glue::glue(
    "
     SELECT *
     FROM { exposureDatabaseSchema }.{ exposureTable }
     WHERE exposure_cohort_definition_id IN (
       { glue::glue_collapse(exposureIds, sep = \", \") }
     )
    "
  )
  sampledCohorts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery
  ) |>
    dplyr::sample_n(
      size = size,
      replace = TRUE
    )

  result <- sampledCohorts |>
    dplyr::rename_with(convertToCamelCase) |>
    dplyr::rename("sourceSubjectId" = "subjectId") |>
    dplyr::arrange(sourceSubjectId) |>
    dplyr::mutate(subjectId = seq_len(dplyr::n())) |>
    dplyr::arrange(
      .data$exposureCohortDefinitionId,
      .data$subjectId,
      .data$sourceSubjectId
    ) |>
    dplyr::select(-cohortDefinitionId) |>
    dplyr::rename("cohortDefinitionId" = "exposureCohortDefinitionId") |>
    dplyr::relocate(subjectId, .before = sourceSubjectId)

  message("Storing sampled cohorts")
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = simulatedCohortTable,
    data = result |> dplyr::rename_with(convertToSnakeCase),
    tempTable = FALSE,
    dropTableIfExists = TRUE
  )

  DatabaseConnector::disconnect(connection)

}

# TODO: implement seed
generatePoissonEventTimes <- function(
  linearPredictor,
  nTimes = 10,
  seed
) {

  n <- nrow(linearPredictor)
  rates <- exp(linearPredictor$linearPredictor)

  times <- matrix(
    floor(rexp(n * nTimes, rate = rep(rates, each = nTimes))),
    nrow = n,
    byrow = TRUE
  )

  linearPredictor |>
    dplyr::mutate(
      draws = split(times, seq_len(n))
    ) |>
    dplyr::select(c("rowId", "draws")) |>
    dplyr::mutate(eventTimes = purrr::map(draws, cumsum)) |>
    dplyr::select(-draws) |>
    tidyr::unnest(eventTimes) |>
    # remove event times after approx 100 years
    dplyr::filter(eventTimes < 365.25 * 100)

}


computePoissonLinearPredictor <- function(model, covariateData) {

  intercept <- model$estimation |>
    dplyr::filter(column_label == 0) |>
    dplyr::pull(estimate)

  intercept <- ifelse(length(intercept == 1), intercept, 0)

  covs_dt <- data.table::as.data.table(
    covariateData$covariates |> dplyr::collect()
  )
  dtCoef <- data.table::as.data.table(model$estimation)
  nonZeroCoef <- dtCoef[dtCoef$estimate != 0, ]

  resDT <- covs_dt[
    nonZeroCoef,
    on = c("covariateId" = "column_label"),
    nomatch = 0L
  ][
    , .(linearPredictor = sum(estimate * covariateValue)),
    by = rowId
  ]

  allRows <- data.table::data.table(rowId = unique(covs_dt$rowId))
  outDT <- base::merge(allRows, resDT, by = "rowId", all.x = TRUE)
  outDT$linearPredictor[is.na(outDT$linearPredictor)] <- 0

  dplyr::as_tibble(outDT) |>
    dplyr::mutate(linearPredictor = linearPredictor + intercept)
}


generateNewOutcomeTable <- function(
  connectionDetails,
  resultDatabaseSchema,
  cohortObservationTable,
  sampledCohortTable,
  resultTable,
  eventTimes,
  outcomeId
) {

  connection <- DatabaseConnector::connect(connectionDetails)

  tableNames <- DatabaseConnector::getTableNames(connection)
  if (!resultTable %in% tableNames) {
    createSql <- glue::glue(
      "
       CREATE TABLE { resultDatabaseSchema }.{ resultTable } (
         COHORT_DEFINITION_ID INTEGER,
         SUBJECT_ID BIGINT,
         COHORT_START_DATE DATE,
         COHORT_END_DATE DATE
       );
       "
    )
    DatabaseConnector::executeSql(connection, createSql)
  }

  patients <- DatabaseConnector::querySql(
    connection,
    glue::glue(
      "
      SELECT
        s.cohort_definition_id,
        s.subject_id,
        s.source_subject_id,
        c.cohort_start_date AS observation_period_start_date,
        c.cohort_end_date AS observation_period_end_date,
        s.cohort_start_date,
        s.cohort_end_date
      FROM { resultDatabaseSchema }.{ sampledCohortTable } s
      JOIN { resultDatabaseSchema }.{ cohortObservationTable } c
        ON c.subject_id = s.source_subject_id
      ORDER BY s.cohort_definition_id, s.subject_id
      "
    )
  ) |>
    dplyr::as_tibble() |>
    dplyr::rename_with(convertToCamelCase) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::ends_with("Date"),
        lubridate::ymd
      )
    )

  result <- eventTimes |>
    dplyr::left_join(
      patients |>
        dplyr::select(
          "subjectId",
          "observationPeriodStartDate",
          "observationPeriodEndDate"
        ),
      by = c("rowId" = "subjectId")
    ) |>
    dplyr::mutate(
      eventDate = observationPeriodStartDate + lubridate::days(eventTimes)
    ) |>
    dplyr::filter(
      eventDate < observationPeriodEndDate
    ) |>
    dplyr::select("rowId", "eventDate") |>
    dplyr::rename(
      c(
        "subjectId" = "rowId",
        "cohortStartDate" = "eventDate"
      )
    ) |>
    dplyr::mutate(
      cohortEndDate = cohortStartDate,
      cohortDefinitionId = outcomeId
    ) |>
    dplyr::relocate(cohortDefinitionId) |>
    dplyr::rename_with(convertToSnakeCase)

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = resultTable,
    data = result,
    createTable = FALSE,
    dropTableIfExists = FALSE
  )

  message("Added lines to table with simulated outcomes")
}
