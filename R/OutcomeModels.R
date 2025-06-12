trainOutcomeModels <- function(
  outcomeModelSettings,
  logSettings = ParallelLogger::createLogSettings(),
  saveDirectory = "./OutcomeModels"
) {


  if (outcomeModelSettings$useSameSplitSettings) {
    populationSettings <- rep(
      list(outcomeModelSettings$populationSettings),
      length(outcomeModelSettings$outcomeIds)
    )
  } else {
    populationSettings <- outcomeModelSettings$populationSettings
  }

  Patientlevelprediction::runMultiplePlp(
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
