limitCdmToCohort <- function(
  connectionDetails,
  cdmDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  outcomeDatabaseSchema,
  outcomeTable,
  resultDatabaseSchema,
  cohortObservationPeriodTable,
  saveDir
) {

  message("Generating database limited to cohorts. This might take a while")

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  subjectIds <- DatabaseConnector::querySql(
    connection = connection,
    sql = glue::glue(
      "
      SELECT DISTINCT subject_id
      FROM { exposureDatabaseSchema }.{ exposureTable }
      ORDER BY subject_id;
      "
    )
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = "temp_subject_ids",
    data = subjectIds,
    tempTable = TRUE
  )

  result <- Andromeda::andromeda()

  message("Limiting tables to cohorts...")

  tableNames <- c(
    "condition_occurrence", "condition_era", "death",
    "device_exposure", "dose_era", "drug_era", "drug_exposure",
    "episode", "measurement", "note", "observation",
    "payer_plan_period", "person", "procedure_occurrence",
    "visit_detail", "visit_occurrence", "observation_period"
  )

  purrr::walk(
    .x = tableNames,
    .f = limitTable,
    connection = connection,
    andromeda = result,
    fromDatabaseSchema = cdmDatabaseSchema,
    resultDatabaseSchema = resultDatabaseSchema,
    subjectIdTable = "temp_subject_ids"
  )

  message("Transferring tables...")

  tableNames <- c(
    "care_site", "cdm_source", "fact_relationship",
    "location", "note_nlp", "provider", "vocabulary",
    "concept", "concept_ancestor", "concept_class",
    "concept_relationship", "concept_synonym"
  )

  purrr::walk(
    .x = tableNames,
    .f = extractTable,
    connection = connection,
    andromeda = result,
    fromDatabaseSchema = cdmDatabaseSchema,
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
    dplyr::select(subject_id, source_subject_id)

  message("Limiting tables to sample...")

  tableNames <- c(
    "condition_occurrence", "condition_era", "death",
    "device_exposure", "dose_era", "drug_era", "drug_exposure",
    "episode", "measurement", "note", "observation", "observation_period",
    "payer_plan_period", "person", "procedure_occurrence",
    "visit_detail", "visit_occurrence"
  )

  purrr::walk(
    .x = tableNames,
    .f = f1,
    fromAndromeda = fromAndromeda,
    toAndromeda = toAndromeda
  )

  message("Extracting required tables...")
  tableNames <- c(
    "care_site", "cdm_source", "fact_relationship",
    "location", "note_nlp", "provider", "vocabulary",
    "concept", "concept_ancestor", "concept_class",
    "concept_relationship", "concept_synonym"
  )
  purrr::walk(
    .x = tableNames,
    .f = f2,
    fromAndromeda = fromAndromeda,
    toAndromeda = toAndromeda
  )

  toAndromeda$person <- toAndromeda$person |>
    dplyr::mutate(
      year_of_birth = as.integer(year_of_birth),
      month_of_birth = as.integer(month_of_birth),
      day_of_birth = as.integer(day_of_birth)
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
