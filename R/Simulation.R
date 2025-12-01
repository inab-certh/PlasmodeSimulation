computeLinearPredictorMatrix <- function(
  featureData,
  models,
  eventId,
  timeId,
  maxLag = 0
) {

  bbData <- models$betas |>
    dplyr::filter(.data[["eventId"]] == !!eventId) |>
    dplyr::collect()

  bb <- models$betas |>
    dplyr::filter(.data[["eventId"]] == !!eventId) |>
    dplyr::pull(.data[["value"]]) |>
    matrix()

  selectedCols <- featureData$colMapping |>
    dplyr::filter(lag <= maxLag) |>
    dplyr::mutate(covariateId = .data[["covariateId"]] + 1) |>
    dplyr::mutate(
      covariateId = dplyr::case_when(
        feature == "timeId" ~ 0,
        TRUE ~ covariateId
      )
    ) |> 
    dplyr::inner_join(bbData, by = "covariateId") |> 
    dplyr::pull(.data[["matrixCol"]])

  selectedRows <- featureData$rowMapping |>
    dplyr::filter(timeId == !!timeId) |>
    dplyr::pull(.data[["matrixRow"]])

  xx <- 1 |>
    cbind(featureData$sparseMatrix[selectedRows, selectedCols])


  result <- xx %*% bb
  attr(result, "eventId") <- eventId

  result

}

extractLinearPredictor <- function(result) {
  # Get row names
  rowNames <- rownames(result)
  splitNames <- strsplit(rowNames, "_")
  rowId <- sapply(splitNames, \(x) x[1])
  timeId <- sapply(splitNames, \(x) x[2])
  eventId <- attr(result, "eventId")

  resultTibble <- dplyr::tibble(
    eventId = eventId %/% 1000,
    rowId = rowId,
    timeId = as.numeric(timeId),
    value = as.vector(result)
  )

  resultTibble
}

simulateBinomialEvents <- function(
  data,
  size = 1,
  seed = as.numeric(Sys.time())
) {
  data |>
    dplyr::mutate(
      prob = plogis(.data[["value"]]),
      event = withr::with_seed(
        seed,
        rbinom(
          n = dplyr::n(),
          size = size,
          prob = .data[["prob"]]
        )
      )
    ) |>
    dplyr::filter(.data[["event"]]  == 1)
}
