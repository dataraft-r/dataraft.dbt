#' @export
#' @importFrom dataraft.core dr_status
dr_status.dr_dbt_result <- function(x, asset = NULL, ...) {
  nodes <- x$results
  rows <- tibble::tibble(
    engine = rep("dbt", nrow(nodes)),
    id = nodes$unique_id,
    status = nodes$status,
    outcome = status_outcome(nodes$status),
    success = nodes$status %in% c("success", "pass", "warn"),
    release_id = rep(NA_character_, nrow(nodes)),
    asset = nodes$unique_id,
    message = rep("", nrow(nodes))
  )
  if (!isTRUE(x$success)) {
    rows <- dplyr::bind_rows(
      rows,
      tibble::tibble(
        engine = "dbt",
        id = ".process",
        status = "error",
        outcome = "failed",
        success = FALSE,
        release_id = NA_character_,
        asset = NA_character_,
        message = "dbt execution or artifact verification failed; inspect the local result."
      )
    )
  }
  return(rows)
}

#' @export
#' @importFrom dataraft.core dr_quality
dr_quality.dr_dbt_result <- function(x, run_id = NULL, asset = NULL, release = NULL, ...) {
  states <- dataraft.core::dr_status(x)
  out <- dplyr::bind_rows(lapply(seq_len(nrow(states)), function(i) {
    node <- states[i, ]
    status <- if (node$status %in% c("pass", "success")) {
      "passed"
    } else if (node$status == "warn") {
      "warning"
    } else if (node$status == "fail") {
      "failed"
    } else if (node$status == "skipped") {
      "not_checked"
    } else {
      "error"
    }
    ix <- match(node$id, x$results$unique_id)
    dataraft.core::dr_internal_quality_row(
      node$id,
      status,
      if (status == "warning") "warning" else "error",
      n_failed = if (is.na(ix)) NA_real_ else x$results$failures[[ix]],
      threshold = NA_real_,
      engine = "dbt",
      stage = "model",
      message = node$message
    )
  }))
  if (!nrow(out)) {
    out <- dataraft.core::dr_internal_quality_row(
      "dbt",
      "not_checked",
      engine = "dbt",
      stage = "model",
      message = "No executed nodes."
    )
  }
  dataraft.core::dr_quality(out)
}

#' @export
#' @importFrom dataraft.core dr_lineage_edges
dr_lineage_edges.dr_dbt_result <- function(x, ...) {
  source <- dr_dbt_lineage(x)
  edges <- tibble::tibble(
    run_id = rep("", nrow(source)),
    from_id = source$from,
    from_version = rep("", nrow(source)),
    to_id = source$to,
    to_version = rep("", nrow(source)),
    relation = rep("dbt_dependency", nrow(source))
  )
  edges
}

#' @export
#' @importFrom dataraft.core dr_lineage_edges
dr_lineage_edges.character <- function(x, ...) {
  dr_lineage_edges.dr_dbt_result(x, ...)
}

status_outcome <- function(status) {
  rlang::local_error_call(rlang::caller_env())
  out <- rep(NA_character_, length(status))
  out[
    status %in% c("completed", "published", "cached", "success", "pass", "warn")
  ] <- "succeeded"
  out[status %in% c("blocked", "fail", "missing")] <- "blocked"
  out[status %in% c("error", "failed", "runtime error")] <- "failed"
  out[status %in% c("skipped", "skip")] <- "skipped"
  out
}


