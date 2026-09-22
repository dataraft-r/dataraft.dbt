#' @export
#' @importFrom dataraft.core dr_replace_source_bindings
dr_replace_source_bindings.dr_dbt_project <- function(x, replacements) {
  rlang::local_error_call(rlang::caller_env())
  if (is.null(x$lake)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_source",
      "Source replacement requires a managed dbt project with lake configuration."
    )
  }
  slots <- list()
  for (group in names(x$source_groups)) {
    for (alias in names(x$source_groups[[group]])) {
      slots[[length(slots) + 1L]] <- c(group = group, alias = alias)
    }
  }
  selected <- integer()
  for (name in names(replacements)) {
    matches <- which(vapply(
      slots,
      function(slot) {
        name == slot[["alias"]] ||
          name == paste(slot[["group"]], slot[["alias"]], sep = ".")
      },
      logical(1)
    ))
    if (!length(matches)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_source",
        paste0(
          "Unknown dbt source binding: ",
          name,
          ". Available names: ",
          paste(
            vapply(
              slots,
              function(slot) paste(slot, collapse = "."),
              character(1)
            ),
            collapse = ", "
          ),
          "."
        )
      )
    }
    if (length(matches) != 1L) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_source",
        paste("Ambiguous dbt source binding; use group.table:", name)
      )
    }
    if (matches %in% selected) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_source",
        "Two replacements select the same dbt source binding."
      )
    }
    selected <- c(selected, matches)
    slot <- slots[[matches]]
    refs <- dbt_source_references(stats::setNames(
      list(replacements[[name]]),
      slot[["alias"]]
    ))
    dbt_source_catalog(refs, x$lake)
    x$source_groups[[slot[["group"]]]][[slot[["alias"]]]] <- refs[[1L]]
  }
  x
}
