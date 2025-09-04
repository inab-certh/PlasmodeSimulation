fitPoissonRegression <- function(
    outcomeId,
    covariateData,
    saveDir,
    prior = Cyclops::createPrior(
      priorType = "laplace",
      variance = 0.1
    ),
    control = Cyclops::createControl(
      cvType = "auto",
      maxIterations = 3000,
      threads = -1
    )) {
  analysisData <- readr::read_csv(
    file = file.path(
      saveDir,
      glue::glue("outcome_{ outcomeId }"),
      "analysisData.csv"
    ),
    show_col_types = FALSE
  )

  rowIds <- covariateData$covariates |>
    dplyr::collect() |>
    dplyr::pull(.data$rowId) |>
    unique()

  outcomes <- dplyr::tibble(rowId = rowIds) |>
    dplyr::left_join(analysisData, by = c("rowId" = "subjectId")) |>
    dplyr::transmute(
      .data$rowId,
      y = .data$outcomeCount,
      time = .data$timeAtRiskDays
    ) |>
    dplyr::filter(.data$time > 0)

  cyclopsData <- Cyclops::convertToCyclopsData(
    outcomes = outcomes,
    covariates = covariateData$covariates |> dplyr::collect(),
    modelType = "pr",
    addIntercept = TRUE
  )

  fit <- Cyclops::fitCyclopsModel(
    cyclopsData = cyclopsData,
    prior = prior,
    control = control,
    warnings = FALSE
  )

  filePath <- file.path(saveDir, glue::glue("outcome_{ outcomeId }"))
  if (!dir.exists(filePath)) dir.create(filePath, recursive = TRUE)
  saveRDS(fit, file.path(filePath, "model.rds"))

  model <- fit$estimation |>
    dplyr::filter(.data$estimate != 0) |>
    dplyr::mutate(exposureId = -1)

  readr::write_csv(model, file.path(filePath, "model.csv"))

  message(glue::glue("Mode saved in { file.path(filePath, 'model.csv') }"))

  modelOverview <- dplyr::tibble(
    outcomeId = outcomeId,
    location = file.path(filePath, "model.csv"),
    return = stringr::str_to_lower(fit$return_flag),
    base = -1
  )

  readr::write_csv(modelOverview, file.path(filePath, "modelOverview.csv"))

  fit
}


#' Train poisson regresion models
#'
#' @description Trains poisson regression models
#'
#' @param connectionDetails The connection details to the database. Should be an
#'     object of type \code{\link[DatabaseConnector]{createConnectionDetails}}.
#' @param cdmDatabaseSchema The database schema where the cdm database is stored.
#' @param exposureDatabaseSchema The database schema where table with the
#'     exposure cohorts are stored.
#' @param exposureTable The table where the exposure cohorts are stored.
#' @param outcomeDatabaseSchema The database schema where table with the outcome
#'     cohorts are stored.
#' @param outcomeTable The table where the outcome cohorts are stored.
#' @param outcomeIds The cohort definition ids of the outcome cohorts.
#' @param covariateSettings The covariates that will be used for fitting the
#'     poisson regression models. An object of type \code{covariateSettings},
#'     generated with \code{\link[FeatureExtraction]{createCovariateSettings}}.
#' @param saveDir The directory where the models will be stored
#' @param ... Additional parameters passed to
#'     \code{\link[Cyclops]{fitCyclopsModel}}
#'
#' @export
trainPoissonModels <- function(
  connectionDetails,
  cdmDatabaseSchema,
  exposureDatabaseSchema,
  exposureTable,
  outcomeDatabaseSchema,
  outcomeTable,
  outcomeIds,
  covariateSettings,
  saveDir,
  ...
) {

  message("Training the outcome models")
  for (outcomeId in outcomeIds) {
    suppressMessages(
      createDirIfNotExists(
        dir = file.path(saveDir, glue::glue("outcome_{ outcomeId }"))
      )
    )
  }

  sameCovariateSettings <- checkIfUsingSameSettings(
    settings      = covariateSettings,
    targetClass   = "covariateSettings",
    nOutcomeIds   = length(outcomeIds),
    settingsLabel = "covariate settings"
  )

  if (sameCovariateSettings) {
    covariateData <- FeatureExtraction::getDbCovariateData(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = exposureTable,
      cohortDatabaseSchema = exposureDatabaseSchema,
      covariateSettings = covariateSettings
    )
    Andromeda::saveAndromeda(
      andromeda = covariateData,
      fileName  = file.path(saveDir, "covariateData")
    )
  } else {
    for (i in seq_along(outcomeIds)) {
      outcomeId <- outcomeIds[i]
      covariateData <- FeatureExtraction::getDbCovariateData(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortTable = exposureTable,
        cohortDatabaseSchema = exposureDatabaseSchema,
        covariateSettings = covariateSettings[[i]]
      )
      Andromeda::saveAndromeda(
        andromeda = covariateData,
        fileName  = file.path(
          saveDir,
          glue::glue("outcome_{outcomeId}"), "covariateData")
      )
    }
  }

  message("Extracting analysis data for all models...")
  purrr::walk(
    .x = outcomeIds,
    .f = extractAnalysisData,
    connectionDetails = connectionDetails,
    outcomeDatabaseSchema = outcomeDatabaseSchema,
    outcomeTable = outcomeTable,
    exposureDatabaseSchema = exposureDatabaseSchema,
    exposureTable = exposureTable,
    saveDir = saveDir,
    .progress = TRUE
  )

  processOutcome <- function(outcomeId, saveDir, ...) {
    covFile <- findFile(
      "covariateData",
      dirs = c(
        file.path(saveDir, glue::glue("outcome_{outcomeId}")),
        saveDir
      )
    )
    covData <- Andromeda::loadAndromeda(covFile)
    on.exit(Andromeda::close(covData))

    fitPoissonRegression(
      outcomeId = outcomeId,
      covariateData = covData,
      saveDir = saveDir,
      ...
    )
  }

  message("Fitting poisson regression models...")
  outcomeIds |>
    purrr::walk(
      purrr::in_parallel(
        \(x) {
          library(PlasmodeSimulation)
          processOutcome(x, saveDir)
        },
        processOutcome = processOutcome,
        saveDir = "models"
      ),
      .progress = TRUE
    )

  message("Generating overview...")

  files <- list.files(
    path = "models",
    pattern = "modelOverview\\.csv$",
    recursive = TRUE,
    full.names = TRUE
  )

  combined <- files |>
    purrr::set_names() |>
    purrr::map_dfr(
      ~ readr::read_csv(.x, show_col_types = FALSE),
      .id = "path"
    ) |>
    dplyr::mutate(
      outcomeId = .data$path |>
        stringr::str_extract("outcome_[^/]+") |>
        stringr::str_remove("^outcome_")
    ) |>
    dplyr::select(outcomeId, dplyr::everything(), -"path")

  readr::write_csv(combined, "models/overview.csv")


  message("Finished training poisson models")
}

extractAnalysisData <- function(
    connectionDetails,
    outcomeDatabaseSchema,
    outcomeTable,
    outcomeId,
    exposureDatabaseSchema,
    exposureTable,
    saveDir) {
  saveDir <- file.path(
    saveDir,
    glue::glue("outcome_{ outcomeId }")
  )

  createDirIfNotExists(saveDir)

  connection <- suppressMessages(DatabaseConnector::connect(connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- glue::glue(
    "
    WITH outcomes AS (
      SELECT *
      FROM { outcomeDatabaseSchema }.{ outcomeTable }
      WHERE cohort_definition_id = { outcomeId }
    )
    SELECT
      c.subject_id,
      COUNT(o.subject_id) outcome_count,
      DATE_DIFF('day', c.cohort_start_date, c.cohort_end_date) time_at_risk_days
    FROM {exposureDatabaseSchema}.{exposureTable} AS c
    LEFT JOIN outcomes AS o
      ON c.subject_id = o.subject_id
     AND o.cohort_start_date BETWEEN c.cohort_start_date AND c.cohort_end_date
    GROUP BY
      c.subject_id,
      c.cohort_start_date,
      c.cohort_end_date
    "
  )

  analysisData <- DatabaseConnector::querySql(connection, sql) |>
    dplyr::rename_with(convertToCamelCase)

  saveFile <- file.path(saveDir, "analysisData.csv")
  readr::write_csv(analysisData, saveFile)
}



#' Modify existing model
#'
#' @description Modify an existing model by updating its coefficients
#'
#' @param modelDir The directory where all the outcome models are stored.
#' @param outcomeId The outcome id of the predicted by the model being modified
#' @param exposureIds The exposure ids for which the model will be modified. If
#'     set to -1, all exposures will be affected the same. Consequently, no
#'     effect modification will be present.
#' @param newBetas A list of data frames with columns \code{column_label} and
#'     \code{estimate}. Column \code{column_label} contains the covariate id for
#'     which the coefficient will be replaced by column \code{estimate}. Should
#'     be the same length as \code{exposureIds}, unless \code{exposureIds} is
#'     -1.
#'
#'
#' @export
modifyExistingModel <- function(
  modelDir,
  outcomeId,
  exposureIds,
  newBetas
) {

  message("Modifying model ...")
  overview <- readr::read_csv(
    file.path(modelDir, "overview.csv"),
    show_col_types = FALSE
  )

  modelReturn <- overview |>
    dplyr::filter(outcomeId == !!outcomeId) |>
    dplyr::pull(.data$return)

  if (modelReturn == "success") {
    model <- readr::read_csv(
      file.path(
        modelDir,
        glue::glue("outcome_{ outcomeId }"),
        "model.csv"
      ),
      show_col_types = FALSE
    )

    newModel <- seq_along(exposureIds) |>
      purrr::map_dfr(
        .f = \(x) {
          newBetas[[x]] |>
            dplyr::rename("newEstimate" = "estimate") |>
            dplyr::right_join(model, by = "column_label") |>
            dplyr::mutate(
              estimate = ifelse(
                is.na(.data$newEstimate),
                .data$estimate,
                .data$newEstimate
              )
            ) |>
            dplyr::select("column_label", "estimate") |>
            dplyr::mutate(exposureId = exposureIds[x]) |>
            dplyr::filter(.data$estimate != 0)
        }
      )

  } else {
    stop(
      glue::glue(
        "Model for outcome { outcomeId } had return value { modelReturn }"
      )
    )
  }

  result <- model |>
    dplyr::bind_rows(newModel)

  maxOutcomeId <- overview |>
    dplyr::pull(outcomeId) |>
    max()

  saveDir <- file.path(
    modelDir,
    glue::glue("outcome_{ maxOutcomeId + 1 }")
  )

  createDirIfNotExists(saveDir)

  readr::write_csv(result, file.path(saveDir, "model.csv"))

  message(glue::glue("New model saved in { file.path(saveDir, 'model.csv') }"))

  addToOverview <- dplyr::tibble(
    outcomeId = maxOutcomeId + 1,
    location = file.path(saveDir, "model.csv"),
    return = "modified",
    base = outcomeId
  )

  overview |>
    dplyr::bind_rows(addToOverview) |>
    dplyr::arrange(outcomeId) |>
    readr::write_csv(file.path(modelDir, "overview.csv"))

  message("Updated overview")
}

trainEventModel <- function(
  population,
  cohortDatabaseSchema,
  cohortTable,
  cohortIds = -1,
  eventId,
  eventData,
  covariateData,
  .s = "lambda.1se",
  modelSettings
) {

  # Get event patients and build outcome
  eventPatients <- eventData$covariates |>
    dplyr::collect() |>
    dplyr::filter(covariateId == eventId)

  patientsWithCovariates <- covariateData$covariates |>
    dplyr::collect() |>
    dplyr::pull(rowId) |>
    unique() |>
    sort()

  y <- population |>
    dplyr::filter(subjectId %in% patientsWithCovariates) |>
    dplyr::left_join(eventPatients, by = c("subjectId" = "rowId")) |>
    dplyr::mutate(
      covariateValue = tidyr::replace_na(covariateValue, 0)
    ) |>
    dplyr::select(subjectId, covariateValue) |>
    dplyr::arrange(subjectId)

  # Validate outcome variation
  n_positive <- sum(y$covariateValue > 0)
  n_total <- nrow(y)

  if (n_positive == 0) {
    stop(
      glue::glue(
        "Cannot train model for eventId {eventId}:",
        "No positive outcomes found ({n_positive}/{n_total}).",
        "Model training requires at least some positive cases."
      )
    )
  }
 
  if (n_positive == n_total) {
    stop(
      glue::glue(
        "Cannot train model for eventId {eventId}:",
        "All outcomes are positive ({n_positive}/{n_total}).",
        "Model training requires outcome variation."
      )
    )
  }
 
  # Optionally warn if very few positive cases
  if (n_positive < 10) {  # or some other threshold
    warning(
      glue::glue(
        "Very few positive outcomes for eventId {eventId}:",
        " {n_positive}/{n_total}.",
        "Model may be unstable."
      )
    )
  }

  # ------------------------------------------------------------------
  # Build feature table X
  # ------------------------------------------------------------------
  x <- covariateData$covariates |>
    dplyr::collect()


  if ("timeId" %in% colnames(covariateData$covariates)) {
    x <- x |>
      dplyr::mutate(
        covariateId = as.numeric(paste0(covariateId, sprintf("%04d", timeId)))
      )
  }

  x$rowId <- as.factor(x$rowId)
  x$covariateId <- as.factor(x$covariateId)

  xSparse <- Matrix::sparseMatrix(
    i = as.integer(x$rowId),
    j = as.integer(x$covariateId),
    x = x$covariateValue,
    dimnames = list(
      levels(x$rowId),
      levels(x$covariateId)
    )
  )

  y <- y |>
    dplyr::mutate(
      subjectId = as.character(subjectId)
    )
  yVec <- setNames(y$covariateValue, y$subjectId)
  yAligned <- yVec[rownames(xSparse)]

  fit <- do.call(
    glmnet::cv.glmnet,
    c(list(x = xSparse, y = yAligned), modelSettings)
  )
 
  betas <- as.data.frame(as.matrix(coef(fit, s = .s))) |>
    tibble::rownames_to_column(var = "covariateId")
  names(betas)[2] <- "value"
  betas <- betas |>
    dplyr::filter(value != 0)
  model <- Andromeda::andromeda()
  model$y <- y |>
    dplyr::rename("rowId" = "subjectId")
  model$eventRef <- eventData$covariateRef |>
    dplyr::filter(covariateId == eventId) |>
    dplyr::collect()
  model$X <- xSparse |>
    Matrix::summary() |>
    dplyr::mutate(
      rowId = rownames(xSparse)[i],
      covariateId = colnames(xSparse)[j]
    ) |>
    dplyr::select(rowId, covariateId, x) |>
    dplyr::rename(value = x) |>
    dplyr::as_tibble() |>
    dplyr::arrange(rowId, covariateId)
  model$betas <- betas
  model$covariateRef <- covariateData$covariateRef |>
    dplyr::collect()
  list(
    model = model,
    fit = fit
  )
}

fitCvGlmnet <- function(
  eventId,
  event = c("covariate", "exposure", "outcome"),
  featureMatrix,
  sample = FALSE,
  sampleSize = 2e4,
  maxTimeId,
  modelSettings,
  .s = "lambda.min",
  seed
) {

  startTime <- Sys.time()
  
  message(
    glue::glue("Fitting model for event { eventId %/% 1000 }. This might take a while...")
  )

  if (missing(seed)) {
    seed <- as.integer(Sys.time())
  }

  modelMatrices <- createModelMatrices(
    featureMatrix = featureMatrix,
    sample = sample,
    sampleSize = sampleSize,
    seed = seed,
    event = event,
    eventId = eventId,
    maxTimeId = maxTimeId
  )

  message("Training model...")
  fit <- do.call(
    glmnet::cv.glmnet,
    c(
      list(
        x = modelMatrices$x,
        y = modelMatrices$y
      ),
      modelSettings
    )
  )


  betas <- as.data.frame(as.matrix(coef(fit, s = .s))) |>
    tibble::rownames_to_column(var = "covariateId")
  names(betas)[2] <- "value"
  betas <- betas |>
    dplyr::filter(value != 0) |>
    dplyr::mutate(
      covariateId = dplyr::case_when(
        covariateId == "(Intercept)" ~ "-1",
        covariateId == "timeId" ~ "0",
        TRUE ~ covariateId
      ),
      covariateId = as.numeric(covariateId),
      eventId = eventId
    ) |>
    dplyr::relocate("eventId")

  model <- Andromeda::andromeda()
  model$betas <- betas
  model$reference <- data.frame(
    event = event,
    eventId = eventId,
    sampled = TRUE,
    seed = seed
  )

  message("Cleaning up...")

  rm(fit)
  gc()

  runDuration <- difftime(Sys.time(), startTime, units = "auto")

  message(
    glue::glue(
      "Model trained in { round(runDuration, 2) } { units(runDuration) }"
    )
  )
  model
}

createModelMatrices <- function(
  featureMatrix,
  sample = TRUE,
  sampleSize = 2e4,
  seed,
  event = c("covariate", "outcome", "exposure"),
  eventId,
  maxTimeId
) {

  if (missing(seed)) {
    seed <- as.integer(Sys.time())
  }

  if (sample) {
    message("Downsampling...")
    sampledRowIds <- featureMatrix$rowMapping |>
      dplyr::pull(rowId) |>
      unique()

    sampledRowIds <- withr::with_seed(
      seed = seed,
      code = sample(x = sampledRowIds, size = sampleSize, replace = FALSE)
    ) |>
      sort()
  } else {
    sampledRowIds <- featureMatrix$rowMapping |>
      dplyr::distinct(rowId) |>
      dplyr::pull(rowId)
  }

  if (event == "covariate") {
    selectedCols <- featureMatrix$colMapping |>
      dplyr::filter(lag != 0)
  } else if (event == "exposure") {
    exposureCols <- featureMatrix$colMapping |>
      dplyr::filter(
        source == "exposures",
        lag != 0
      )

    covariateCols <- featureMatrix$colMapping |>
      dplyr::filter(
        source == "covariates"
      )

    outcomeCols <- featureMatrix$colMapping |>
      dplyr::filter(
        source == "outcomes",
        lag != 0
      )

    selectedCols <- covariateCols |>
      dplyr::bind_rows(exposureCols, outcomeCols)

  } else if (event == "outcome") {

    exposureCols <- featureMatrix$colMapping |>
      dplyr::filter(
        source == "exposures"
      )

    covariateCols <- featureMatrix$colMapping |>
      dplyr::filter(
        source == "covariates"
      )

    outcomeCols <- featureMatrix$colMapping |>
      dplyr::filter(
        source == "outcomes",
        lag != 0
      )

    selectedCols <- covariateCols |>
      dplyr::bind_rows(exposureCols, outcomeCols)

  }

  xMatrix <- featureMatrix$sparseMatrix[
    featureMatrix$rowMapping |>
      dplyr::filter(
        timeId < maxTimeId,
        rowId %in% sampledRowIds
      ) |>
      dplyr::pull(matrixRow),
    selectedCols |>
      dplyr::pull(matrixCol)
  ]

  yMatrix <- featureMatrix$sparseMatrix[
    featureMatrix$rowMapping |>
      dplyr::filter(
        timeId < maxTimeId,
        rowId %in% sampledRowIds
      ) |>
      dplyr::pull(matrixRow),
    featureMatrix$colMapping |>
      dplyr::filter(
        lag == 0,
        covariateId == eventId
      ) |>
      dplyr::pull(matrixCol)
  ]

  list(
    x = xMatrix,
    y = yMatrix
  )
}
