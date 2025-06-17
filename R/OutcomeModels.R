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
                           connection
) {

  return(TRUE)

}


simulateOutcomes <- function(
  x1,
  x2,
  ...
) {

  return(TRUE)

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
