trainAllModels <- function(
  connectionDetails,
  resultDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  exposureIds,
  databaseDetails,
  outcomeModelSettings,
  censoringModelSettings,
  censoringTime,
  saveDir
) {

  message("Start training of all models...")

  extractCohorts(
    connectionDetails = connectionDetails,
    exposureTable = exposureTable,
    exposureIds = exposureIds,
    cohortTable = "combined_target",
    resultDatabaseSchema = resultDatabaseSchema,
    exposureDatabaseSchema = exposureDatabaseSchema,
    cdmDatabaseSchema = databaseDetails$cdmDatabaseSchema
  )

  outcomeModelSaveDir <- file.path(saveDir, "outcomes")

  message("Training outcome models...")
  trainModels(
    modelSettings = outcomeModelSettings,
    databaseDetails = databaseDetails,
    saveDirectory = outcomeModelSaveDir
  )


  message("Training censoring models...")

  generateCensoringTable(
    connectionDetails = connectionDetails,
    censoringTime = censoringTime,
    exposureDatabaseSchema = exposureDatabaseSchema,
    exposureTable = "combined_target",
    outcomeDatabaseSchema = databaseDetails$outcomeDatabaseSchema,
    outcomeTable = databaseDetails$outcomeTable,
    outcomeIds = outcomeModelSettings$outcomeIds,
    resultDatabaseSchema = resultDatabaseSchema,
    resultTable = "combined_censoring"
  )

  censoringModelSaveDir <- file.path(saveDir, "censoring")
  trainModels(
    modelSettings = censoringModelSettings,
    databaseDetails = censoringDatabaseDetails,
    saveDirectory = censoringModelSaveDir
  )

  message("Checking outcome model status...")
  outcomeSettings <- checkEventModels(outcomeModelSaveDir) |>
    dplyr::mutate(model = "outcome")

  message("Checking censoring model status...")
  censoringSettings <- checkEventModels(censoringModelSaveDir) |>
    dplyr::mutate(model = "censoring")

  outcomeSettings |>
    dplyr::bind_rows(censoringSettings) |>
    dplyr::relocate("model") |>
    readr::write_csv(file.path(saveDir, "overview.csv"))

  message("Finished training models")

}


trainModels <- function(
  modelSettings,
  databaseDetails,
  logSettings = PatientLevelPrediction::createLogSettings(),
  saveDirectory = "./OutcomeModels"
) {

  outcomeIds <- modelSettings$outcomeIds

  modelDesignList <- outcomeIds |>
    purrr::map(
      ~ list(
        targetId = modelSettings$targetId,
        outcomeId = .x
      )
    ) |>
    purrr::set_names(paste0("outcomeId_", outcomeIds))

  components <- list(
    executeSettings = list(
      flag = modelSettings$useSameExecuteSettings,
      value = modelSettings$executeSettings
    ),
    modelSettings = list(
      flag = modelSettings$useSameModelSettings,
      value = modelSettings$modelSettings
    ),
    splitSettings = list(
      flag = modelSettings$useSameSplitSettings,
      value = modelSettings$splitSettings
    ),
    sampleSettings = list(
      flag = modelSettings$useSameSampleSettings,
      value = modelSettings$sampleSettings
    ),
    covariateSettings = list(
      flag = modelSettings$useSameCovariateSettings,
      value = modelSettings$covariateSettings
    ),
    populationSettings = list(
      flag = modelSettings$useSamePopulationSettings,
      value = modelSettings$populationSettings
    ),
    preprocessSettings = list(
      flag = modelSettings$useSamePreprocessSettings,
      value = modelSettings$preprocessSettings
    ),
    restrictPlpDataSettings = list(
      flag = modelSettings$useSameRestrictPlpDataSettings,
      value = modelSettings$restrictPlpDataSettings
    ),
    featureEngineeringSettings = list(
      flag = modelSettings$useSameFeatureEngineeringSettings,
      value = modelSettings$featureEngineeringSettings
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


checkEventModels <- function(directory) {

  .check <- function(directory) {
    checkDir <- file.path(directory, "plpResult")
    if (dir.exists(checkDir)) {
      model <- PatientLevelPrediction::loadPlpModel(
        file.path(checkDir, "model")
      )
      if (model$model$modelStatus != "OK") {
        FALSE
      } else {
        TRUE
      }
    } else {
      FALSE
    }
  }

  directory <- gsub("/+$", "", directory)

  settings <- readr::read_csv(
    file.path(directory, "settings.csv"),
    show_col_types = FALSE
  )

  analysisIds <- settings |>
    dplyr::pull(analysisId)

  checkDirectories <- glue::glue(
    "{ directory }/{ analysisIds }"
  )

  checkResults <- purrr::map_lgl(checkDirectories, .check)

  result <- dplyr::tibble(
    analysisId = analysisIds,
    directory = checkDirectories,
    checkPassed = checkResults
  ) |>
    dplyr::mutate(directory = as.character(directory))

  if (any(!result$checkPassed)) {
    failedAnalysisIds <- result |>
      dplyr::filter(!checkPassed) |>
      dplyr::pull(analysisId)
    message(
      glue::glue(
        "Check failed for analyses : {
           glue::glue_collapse(failedAnalysisIds, sep = ', ')
         }"
      )
    )
  } else {
    message("All models passed check")
  }

  settings |>
    dplyr::left_join(result, by = "analysisId")

}
