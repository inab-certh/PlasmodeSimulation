addSettings <- function(
  settings,
  flagSame,
  settingsName,
  settingsValue
) {
  if (flagSame) {
    purrr::map(
      settings,
      ~ append(.x, purrr::set_names(list(settingsValue), settingsName))
    )
  } else {
    purrr::map2(
      settings,
      settingsValue,
      ~ append(.x, purrr::set_names(list(.y), settingsName))
    )
  }
}
