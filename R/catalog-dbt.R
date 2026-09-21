#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @name dbt_catalog_warning

dbt_catalog_warning <- function(delivery) {
  rlang::local_error_call(rlang::caller_env())
  rlang::warn(
    paste(delivery$message, "The dbt outcome is unchanged."),
    class = "dr_dbt_catalog_delivery",
    delivery = delivery
  )
}


dbt_artifact_hashes <- function(path) {
  rlang::local_error_call(rlang::caller_env())
  files <- c("manifest.json", "run_results.json", "catalog.json")
  files <- files[file.exists(file.path(path, files))]
  stats::setNames(
    vapply(
      file.path(path, files),
      digest::digest,
      character(1),
      algo = "sha256",
      file = TRUE
    ),
    files
  )
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @name dbt_catalog_artifacts

dbt_catalog_artifacts <- function(result, path = result$artifacts_dir) {
  rlang::local_error_call(rlang::caller_env())
  if (
    is.null(result$invocation_id) ||
      is.null(result$artifact_hashes) ||
      !is.character(result$artifact_hashes) ||
      !all(
        c("manifest.json", "run_results.json") %in%
          names(result$artifact_hashes)
      )
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "The result has no verified dbt artifact identity.",
      "dr_dbt_artifact_invalid"
    )
  }
  parsed <- dbt_read_artifacts(path)
  if (
    !identical(parsed$manifest$metadata$invocation_id, result$invocation_id) ||
      !identical(dbt_artifact_hashes(path), result$artifact_hashes)
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "The dbt artifacts changed after execution.",
      "dr_dbt_artifact_invalid"
    )
  }
  if ("catalog.json" %in% names(result$artifact_hashes)) {
    catalog <- dbt_read_json(file.path(path, "catalog.json"))
    if (
      !is.list(catalog$nodes) ||
        !identical(catalog$metadata$invocation_id, result$invocation_id)
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_dbt",
        "catalog.json must belong to the same dbt invocation.",
        "dr_dbt_artifact_invalid"
      )
    }
  }
  invisible(TRUE)
}
