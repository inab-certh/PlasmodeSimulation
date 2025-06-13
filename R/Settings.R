checkIfUsingSameSettings <- function(
  settings,
  targetClass,
  nOutcomeIds,
  settingsLabel = "label"
) {

  if (inherits(settings, targetClass)) {
    message(glue::glue("Using same {settingsLabel} for all outcomes"))
    return(TRUE)
  } else if (is.list(settings)) {
    if (length(settings) == nOutcomeIds) {
      message(glue::glue("Using different {settingsLabel} for each outcome"))
      return(FALSE)
    } else {
      stop(glue::glue("Length of {settingsLabel} list does not match number of outcomes"))
    }
  }


  stop(glue::glue("{settingsLabel} must be a list or a '{targetClass}' object"))
}

createOutcomeModelSettings <- function(
  databaseDetails,
  targetId = 1,
  outcomeIds,
  populationSettings,
  restrictPlpDataSettings,
  covariateSettings,
  featureEngineeringSettings,
  sampleSettings,
  splitSettings,
  preprocessSettings,
  modelSettings,
  saveDirectory = "./OutcomeModels"
) {

  message("Generating outcome model settings ...")
  nOutcomeIds <- length(outcomeIds)

  meta <- list(
    settings = list(
      populationSettings,
      restrictPlpDataSettings,
      covariateSettings,
      featureEngineeringSettings,
      sampleSettings,
      splitSettings,
      preprocessSettings,
      modelSettings
    ),
    targetClass = c(
      "populationSettings",
      "restrictPlpDataSettings",
      "covariateSettings",
      "featureEngineeringSettings",
      "sampleSettings",
      "splitSettings",
      "preprocessSettings",
      "modelSettings"
    ),
    settingsLabel = c(
      "population settings",
      "restrict plp data settings",
      "covariate settings",
      "feature engineering settings",
      "sample settings",
      "split settings",
      "pre-process settings",
      "model settings"
    )
  )

  useSameFlags <- purrr::pmap_lgl(
    .l = meta,
    .f = checkIfUsingSameSettings,
    nOutcomeIds = nOutcomeIds
  )

  names(useSameFlags) <- paste0(
    "useSame",
    c(
      "PopulationSettings",
      "RestrictPlpDataSettings",
      "CovariateSettings",
      "FeatureEngineeringSettings",
      "SampleSettings",
      "SplitSettings",
      "PreprocessSettings",
      "ModelSettings"
    )
  )

  result <- c(
    list(
      databaseDetails = databaseDetails,
      targetId = targetId,
      outcomeIds = outcomeIds,
      populationSettings = populationSettings,
      restrictPlpDataSettings = restrictPlpDataSettings,
      covariateSettings = covariateSettings,
      featureEngineeringSettings = featureEngineeringSettings,
      sampleSettings = sampleSettings,
      splitSettings = splitSettings,
      preprocessSettings = preprocessSettings,
      modelSettings = modelSettings
    ),
    as.list(useSameFlags),
    list(saveDirectory = saveDirectory)
  )

  class(result) <- "outcomeModelSettings"
  message("Done")

  result

}
