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
        stop(
          glue::glue(
            "Length of {settingsLabel} list does not match number of outcomes"
          )
      )
    }
  }

  stop(glue::glue("{settingsLabel} must be a list or a '{targetClass}' object"))
}
