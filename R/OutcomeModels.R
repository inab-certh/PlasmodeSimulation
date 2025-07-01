trainOutcomeModels <- function(
  outcomeModelSettings,
  databaseDetails,
  logSettings = PatientLevelPrediction::createLogSettings(),
  saveDirectory = "./OutcomeModels"
) {

  outcomeIds <- outcomeModelSettings$outcomeIds
  targetId <- outcomeModelSettings$targetId

  modelDesignList <- outcomeIds |>
    purrr::map(
      ~ list(
        targetId = outcomeModelSettings$targetId,
        outcomeId = .x
      )
    ) |>
    purrr::set_names(paste0("outcomeId_", outcomeIds))

  components <- list(
    executeSettings = list(
      flag = outcomeModelSettings$useSameExecuteSettings,
      value = outcomeModelSettings$executeSettings
    ),
    modelSettings = list(
      flag = outcomeModelSettings$useSameModelSettings,
      value = outcomeModelSettings$modelSettings
    ),
    splitSettings = list(
      flag = outcomeModelSettings$useSameSplitSettings,
      value = outcomeModelSettings$splitSettings
    ),
    sampleSettings = list(
      flag = outcomeModelSettings$useSameSampleSettings,
      value = outcomeModelSettings$sampleSettings
    ),
    covariateSettings = list(
      flag = outcomeModelSettings$useSameCovariateSettings,
      value = outcomeModelSettings$covariateSettings
    ),
    populationSettings = list(
      flag = outcomeModelSettings$useSamePopulationSettings,
      value = outcomeModelSettings$populationSettings
    ),
    preprocessSettings = list(
      flag = outcomeModelSettings$useSamePreprocessSettings,
      value = outcomeModelSettings$preprocessSettings
    ),
    restrictPlpDataSettings = list(
      flag = outcomeModelSettings$useSameRestrictPlpDataSettings,
      value = outcomeModelSettings$restrictPlpDataSettings
    ),
    featureEngineeringSettings = list(
      flag = outcomeModelSettings$useSameFeatureEngineeringSettings,
      value = outcomeModelSettings$featureEngineeringSettings
    )
  )

  modelDesignList <- purrr::reduce(
    names(components),
    .init = modelDesignList,
    .f = function(s, n) {
      addSettings(
        settings = s,
        flagSame = components[[n]]$flag,
        settingsName = n,
        settingsValue = components[[n]]$value
      )
    }
  )

  PatientLevelPrediction::runMultiplePlp(
    databaseDetails = databaseDetails,
    modelDesignList = modelDesignList,
    onlyFetchData = FALSE,
    logSettings = logSettings,
    saveDirectory = saveDirectory
  )

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

  subjectIds <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery
  ) |>
    unlist(use.names = FALSE)

  if (size > length(subjectIds)) {
    warning("Size of simulated dataset larger than original population")
  }

  subjectIds <- subjectIds |>
    sample(size = size, replace = TRUE)

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = "temp_sampled_subject_ids",
    data = data.frame(subject_id = subjectIds),
    tempTable = TRUE
  )

  # Selects only the first drug exposure for each patient
  # If a patient is present in two or more exposure cohorts
  # Only the earliest is selected
  sqlQuery <- glue::glue(
    "
    SELECT
      cohort_definition_id,
      subject_id,
      cohort_start_date,
      cohort_end_date
    FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER (
          PARTITION BY subject_id ORDER BY cohort_start_date ASC
        ) as rn
      FROM { exposureDatabaseSchema }.{ exposureTable }
      WHERE subject_id IN (
        SELECT subject_id FROM { resultDatabaseSchema }.temp_sampled_subject_ids
      )
      AND cohort_definition_id IN (
        { glue::glue_collapse(exposureIds, sep = \", \") }
      )
    ) sub
    WHERE rn = 1
    ORDER BY cohort_definition_id, subject_id, cohort_start_date
    "
  )

  sampledCohorts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlQuery
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = resultDatabaseSchema,
    tableName = simulatedCohortTable,
    data = sampledCohorts,
    tempTable = FALSE,
    dropTableIfExists = TRUE
  )

  message("Sampled cohorts stored in table sampled_cohorts")

  DatabaseConnector::disconnect(connection)

}


predictOnSample <- function(
  connectionDetails,
  modelDir,
  cdmDatabaseSchema,
  outcomeDatabaseSchema,
  outcomeTable,
  outcomeIds,
  resultDatabaseSchema,
  sampledCohortsTable = "sampled_cohorts",
  exposureIds
) {

  settings <- readr::read_csv(
    file.path(modelDir, "settings.csv"),
    show_col_types = FALSE
  )

  databaseDetails <- PatientLevelPrediction::createDatabaseDetails(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = resultDatabaseSchema,
    cohortTable = sampledCohortsTable,
    outcomeDatabaseSchema = outcomeDatabaseSchema,
    outcomeTable = outcomeTable
  )

  for (i in seq_along(exposureIds)) {

    thisTargetId <- exposureIds[i]
    message(glue::glue("Predicting on targetId { thisTargetId }"))


    for (j in seq_along(outcomeIds)) {

      databaseDetails$targetId <- thisTargetId
      databaseDetails$outcomeIds <- thisOutcomeId

      thisOutcomeId <- outcomeIds[j]
      message(glue::glue("Outcome id { thisOutcomeId }"))
      if (thisOutcomeId %in% settings$outcomeId) {

        thisOutcomeAnalysisId <- settings |>
          dplyr::filter(outcomeId == thisOutcomeId) |>
          dplyr::pull(analysisId)

        thisPlpResult <- PatientLevelPrediction::loadPlpModel(
          file.path(modelDir, thisOutcomeAnalysisId, "plpResult/model")
        )

        thisValidationDesign <- PatientLevelPrediction::createValidationDesign(
          targetId = thisTargetId,
          outcomeId = thisOutcomeId,
          plpModelList = list(thisPlpResult)
        )
      } else {
        stop(glue::glue("No model found for outcome id: { outcomeIds[i] }"))
      }

      thisValidationSaveDir <- glue::glue(
        "targetId_{ thisTargetId }_outcomeId_{ thisOutcomeId }"
      )

      PatientLevelPrediction::validateExternal(
        validationDesignList = thisValidationDesign,
        databaseDetails = databaseDetails,
        outputFolder = file.path(modelDir, "sample", thisValidationSaveDir)
      )
    }
  }
}


simulateOutcomes <- function(
  prediction,
  baselineSurvival,
  seed = 1
) {


  seeds <- withr::with_seed(seed, sample(1:1e8, dim(prediction)[1]))
  baselineSurvivalAtPrediction <- baselineSurvival$surv |>
    tail(1)

  linearPredictor <- log(
    log(1 - prediction$value) /
      log(baselineSurvivalAtPrediction)
  )


  result <- purrr::map2_dbl(
    .x = linearPredictor,
    .y = seeds,
    .f = simulateEventForPerson,
    baselineSurvival = baselineSurvival$surv,
    times = baselineSurvival$time
  )

  data.frame(
    rowId = prediction$rowId,
    subjectId = prediction$subjectId,
    time = result
  )

}


simulateEventForPerson <- function(
  linearPredictor,
  baselineSurvival,
  times,
  seed
) {

  if (missing(seed)) {
    seed <- sample(1:1e8, 1)
    message(glue::glue("No seed provided. Using seed: { seed }"))
  }

  if (length(times) != length(baselineSurvival)) {
    stop("Differing lengths of times and baselineSurvival!")
  }

  randomUnif <- withr::with_seed(seed, runif(1))
  person <- baselineSurvival ^ exp(linearPredictor)
  idx <- which(person < randomUnif)
  if (length(idx) == 0) {
    eventTime <- Inf
  } else {
    eventTime <- times[which(person < randomUnif) |> min()]
  }

  eventTime

}

runPlasmodeSimulation <- function(
  outcomeModelSettings,
  samplSettings,
  databaseSettings,
  ...
) {

  # Train the outcome models
  # Draw a random sample from existing cohorts
  # Predict outcome probabilities in sampled patients
  # Create simulated outcome table

  return(TRUE)

}

updateEventPrediction <- function(
  databaseDetails,
  modelSettings,
  modelDir,
  timepoint,
  outcomeId,
  newCoefficients,
  newBaselineSurvival
) {

  databaseDetails$targetId <- 1
  databaseDetails$outcomeIds <- outcomeId
  plpData <- PatientLevelPrediction::getPlpData(
    databaseDetails = databaseDetails,
    covariateSettings = modelSettings$covariateSettings
  )

  population <- PatientLevelPrediction::createStudyPopulation(
    plpData = plpData,
    outcomeId = outcomeId,
    populationSettings = modelSettings$populationSettings
  )

  plpModel <- PatientLevelPrediction::loadPlpModel(modelDir)


  if (missing(newCoefficients)) {
    newCoefficients <- plpModel$model$coefficients
    message("Using existing model coefficients")
  }

  if (missing(newBaselineSurvival)) {
    newBaselineSurvival <- plpModel$model$baselineSurvival
    message("Using existing baseline survival")
  }

  plpModel$model$coefficients <- newCoefficients
  plpModel$model$baselineSurvival <- newBaselineSurvival

  prediction <- PatientLevelPrediction::predictPlp(
    plpModel = plpModel,
    plpData = plpData,
    population = population,
    timepoint = timepoint
  )

  list(
    prediction = prediction,
    coefficitents = newCoefficients,
    baselineSurvival = newBaselineSurvival,
    covariates = plpData$covariates
  )

}



generateCensoringTable <- function(
  connectionDetails,
  censoringTime,
  exposureDatabaseSchema,
  exposureTable,
  outcomeDatabaseSchema,
  outcomeTable,
  outcomeIds,
  resultDatabaseSchema,
  resultTable
) {

  connection <- DatabaseConnector::connect(connectionDetails)
  buildSelectSql <- function(outcomeId) {
    glue::glue(
      "
      WITH censor_base AS (
        SELECT
          exposure_cohort_definition_id,
          subject_id,
          cohort_start_date AS exp_start,
          cohort_end_date,
          CASE
            WHEN cohort_end_date < DATE_ADD(cohort_start_date, INTERVAL '{censoringTime} days')
              THEN cohort_end_date
            ELSE DATE_ADD(cohort_start_date, INTERVAL '{censoringTime} days')
          END AS censor_date
        FROM {exposureDatabaseSchema}.{exposureTable}
      )
      SELECT
        {outcomeId} AS cohort_definition_id,
        exposure_cohort_definition_id,
        cb.subject_id,
        cb.censor_date        AS cohort_start_date,
        cb.censor_date        AS cohort_end_date
      FROM censor_base cb
      WHERE NOT EXISTS (
        SELECT 1
        FROM {outcomeDatabaseSchema}.{outcomeTable} o
        WHERE o.subject_id           = cb.subject_id
          AND o.cohort_definition_id = {outcomeId}
          AND o.cohort_start_date   >= cb.exp_start
          AND o.cohort_start_date   <= cb.censor_date
      )
      "
    )
  }

  tableNames <- DatabaseConnector::getTableNames(connection)
  if (!resultTable %in% tableNames) {
    createSql <- glue::glue(
      "
       CREATE TABLE { resultDatabaseSchema }.{ resultTable } (
         cohort_definition_id          INTEGER,
         exposure_cohort_definition_id INTEGER,
         subject_id                    BIGINT,
         cohort_start_date             DATE,
         cohort_end_date               DATE
       );
       "
    )
    DatabaseConnector::executeSql(connection, createSql)
  }

  for (i in seq_along(outcomeIds)) {
    outcomeId <- outcomeIds[i]
    selectSql <- buildSelectSql(outcomeId)

    insertSql <- glue::glue(
      "INSERT INTO {resultDatabaseSchema}.{resultTable}\n{selectSql}"
    )
    DatabaseConnector::executeSql(connection, insertSql)
    message(sprintf(
      "Appended outcomeId = %s to %s.%s",
      outcomeId, resultDatabaseSchema, resultTable
    ))
  }
  message("All outcomeIds processed.")
  DatabaseConnector::disconnect(connection)
}
