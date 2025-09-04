generateEventData <- function(
  connection,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  cohortTable,
  period,
  covariateSettings,
  exposureSettings,
  outcomeSettings,
  periodLength = 30
) {
  # Helper function to get covariate data
  .getCovData <- function(settings, useNextPeriod = FALSE) {
    if (useNextPeriod) {
      settings <- createNextPeriodSettings(settings, periodLength)
    }
    suppressMessages(
      FeatureExtraction::getDbCovariateData(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        covariateSettings = settings,
        cohortIds = period
      )
    )
  }

  # Helper function to get cohort-based covariate data
  .getCohortCovData <- function(settings) {
    suppressMessages(
      FeatureExtraction::getDbCohortBasedCovariatesData(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortTable = cohortTable,
        cohortIds = period,
        covariateSettings = settings
      )
    )
  }

  covariateData <- .getCovData(covariateSettings)
  exposureData <- .getCohortCovData(exposureSettings)
  outcomeData <- .getCohortCovData(outcomeSettings)

  covariateDataNext <- .getCovData(covariateSettings, TRUE)
  exposureDataNext <- .getCovData(exposureSettings, TRUE)
  outcomeDataNext <- .getCovData(outcomeSettings, TRUE)

  X_base <- combineTemporalCovariates(
    covariateDataList = list(covariateData, exposureData, outcomeData)
  )

  list(
    history = list(
      covariateData = covariateData,
      exposureData = exposureData,
      outcomeData = outcomeData
    ),
    future = list(
      covariateData = covariateDataNext,
      exposureData = exposureDataNext,
      outcomeData = outcomeDataNext
    )
  )

  # switch(event,
  #   "exposure" = list(
  #     X = X_base,
  #     y = exposureDataNext
  #   ),
  #   "outcome" = list(
  #     X = appendTemporalCovariates(X_base, exposureDataNext),
  #     y = outcomeDataNext
  #   ),
  #   "covariate" = list(
  #     X = appendTemporalCovariates(
  #       X_base,
  #       combineTemporalCovariates(
  #         covariateDataList = list(exposureDataNext, outcomeDataNext)
  #       )
  #     ),
  #     y = covariateDataNext
  #   )
  # )
}


combineTemporalCovariates <- function(covariateDataList) {
  collectBind <- function(name) {
    covariateDataList |>
      purrr::map(~ .x[[name]]) |>
      purrr::compact() |>
      purrr::map(dplyr::collect) |>
      dplyr::bind_rows()
  }

  timeRefList <- covariateDataList |>
    purrr::map(~ .x[["timeRef"]]) |>
    purrr::compact() |>
    purrr::map(dplyr::collect)

  if (length(timeRefList) > 1) {
    areIdentical <- purrr::map_lgl(
      timeRefList[-1],
      ~ identical(.x, timeRefList[[1]])
    )

    if (!all(areIdentical)) {
      stop("All timeRef dataframes must be identical")
    }
  }

  result <- Andromeda::andromeda()

  result$analysisRef <- collectBind("analysisRef")
  result$covariateRef <- collectBind("covariateRef")
  result$covariates <- collectBind("covariates")
  result$timeRef <- if (length(timeRefList) > 0) {
    timeRefList[[1]]
  } else {
    data.frame()
  }

  result
}

buildSparseTemporal <- function(df, allTimeIds = NULL) {
  df2 <- df |>
    dplyr::mutate(
      baseId = covariateId %/% 1000L,
      lag    = covariateId %% 1000L
    ) |>
    dplyr::filter(lag %in% c(0L, 1L)) |>
    dplyr::mutate(
      rowKey = paste(rowId, timeId, sep = "_"),
      colKey = as.character(covariateId)
    ) |>
    dplyr::distinct(rowKey, colKey, .keep_all = TRUE) |>
    dplyr::select(rowKey, colKey, covariateValue, rowId, timeId, baseId, lag)

  if (is.null(allTimeIds)) {
    ranges <- df2 |>
      dplyr::distinct(rowId, timeId) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(
        minTime = min(timeId),
        maxTime = max(timeId),
        .groups = "drop"
      ) |>
      dplyr::arrange(rowId)

    rowLevels <- unlist(
      lapply(seq_len(nrow(ranges)), function(k) {
        paste(
          ranges$rowId[k],
          seq.int(ranges$minTime[k], ranges$maxTime[k]),
          sep = "_"
        )
      }),
      use.names = FALSE
    )
  } else {
    rowIds <- sort(unique(df2$rowId))
    rowLevels <- as.vector(
      t(outer(rowIds, allTimeIds, function(r, t) paste(r, t, sep = "_")))
    )
  }

  bases <- sort(unique(df2$baseId))
  colLevels <- as.character(c(rbind(bases * 1000L, bases * 1000L + 1L)))

  # index maps
  rowMap <- stats::setNames(seq_along(rowLevels), rowLevels)
  colMap <- stats::setNames(seq_along(colLevels), colLevels)

  # indices for observed entries (zeros are implicit)
  i <- unname(rowMap[df2$rowKey])
  j <- unname(colMap[df2$colKey])
  x <- df2$covariateValue

  # sparse covariate matrix with full row grid
  X <- Matrix::sparseMatrix(
    i = i, j = j, x = x,
    dims = c(length(rowLevels), length(colLevels)),
    dimnames = list(rowLevels, colLevels)
  )

  # row mapping back to (rowId, timeId)
  rowMapping <- tibble::tibble(
    matrixRow = seq_along(rowLevels),
    rowKey = rowLevels
  ) |>
    tidyr::separate(
      rowKey,
      into = c("rowId", "timeId"), sep = "_", convert = TRUE
    )

  # Append timeId as the LAST COLUMN
  timeColSparse <- Matrix::sparseMatrix(
    i = seq_len(nrow(X)),
    j = rep_len(1L, nrow(X)),
    x = rowMapping$timeId,
    dims = c(nrow(X), 1L),
    dimnames = list(rownames(X), "timeId")
  )
  X <- cbind(X, timeColSparse)

  # column dictionary
  colMapping <- tibble::tibble(
    matrixCol  = seq_len(ncol(X)),
    feature = c(rep("covariate", length(colLevels)), "timeId"),
    covariateId = c(as.numeric(colLevels), NA_real_),
    baseId = c(as.numeric(colLevels) %/% 1000L, NA_real_),
    lag = c(as.numeric(colLevels) %% 1000L, -1)
  )

  list(
    sparseMatrix = X,
    rowMapping = rowMapping,
    colMapping = colMapping
  )
}

createLaggedCovariatesWithMapping <- function(
  andromeda,
  maxLag = 1,
  lagIdMultiplier = 1000
) {
  # Start with original data
  result <- andromeda |>
    dplyr::mutate(covariateId = covariateId * lagIdMultiplier)

  for (lag in 1:maxLag) {
    # Create lag by self-joining with time shift
    laggedData <- andromeda |>
      dplyr::mutate(
        timeId = timeId + 1,
        covariateId = covariateId * lagIdMultiplier + lag
      )
    
    # Add to result
    result <- dplyr::union_all(result, laggedData)
  }
  
  result |>
    dplyr::arrange(rowId, timeId, covariateId)
}
