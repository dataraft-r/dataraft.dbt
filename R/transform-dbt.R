#' Transform staged product input with a dbt model
#'
#' Writes the incoming table to an explicitly named staging relation, closes
#' that connection, runs `dbt build` for the selected model, then opens a fresh
#' connection to read the successfully built relation. The dbt model must
#' reference the supplied staging table; this adapter never ignores its input.
#'
#' `input` is replaced on every execution. Use a dedicated staging table that
#' is not managed by a dbt seed or model, and coordinate concurrent writers.
#' dbt owns SQL dependencies, tests and incremental strategies. Staging and dbt
#' model changes are external side effects; the product's final quality gate
#' controls its publication target, not a rollback of dbt's database changes.
#' The returned table is materialized so every factory-owned connection closes.
#' Invocation ID and selected model are attached as `dr_transform_metadata`.
#' @param project A [dr_dbt_project()] specification.
#' @param model Exact materialized dbt model unique ID, for example
#'   `"model.shop.customer_revenue"`.
#' @param connection Zero-argument function opening a DBI connection to the
#'   database used by dbt. Return a new connection on every call.
#' @param input Dedicated staging table, as a string or [DBI::Id()]. Existing
#'   table contents are replaced. Its schema must already exist.
#' @param full_refresh,vars,echo,timeout Passed to [dr_dbt_build()].
#' @returns A deferred transformation adapter for [dataraft.core::dr_step_transform()].
#' @seealso [dr_dbt_init()], [dr_dbt_build()], [dataraft.adapters::dr_source_database()]
#' @export
#' @examples
#' # dbt's selected model must read the dedicated staged_orders relation.
#' project <- dr_dbt_project("analytics", profiles_dir = "analytics")
#' step <- dr_transform_dbt(project, "model.shop.customer_revenue",
#'   connection = function() DBI::dbConnect(duckdb::duckdb(), "analytics.duckdb"),
#'   input = "staged_orders")
#' dataraft.core::dr_inspect(step)
dr_transform_dbt <- function(
  project,
  model,
  connection,
  input,
  full_refresh = FALSE,
  vars = list(),
  echo = FALSE,
  timeout = Inf
) {
  if (!inherits(project, "dr_dbt_project")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "project must come from dr_dbt_project()."
    )
  }
  dataraft.core::dr_internal_scalar(model, "model")
  if (
    !grepl("^model\\.[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*$", model)
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "model must be an exact dbt unique ID such as 'model.shop.orders'."
    )
  }
  if (!is.function(connection)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "connection must be a function opening a new DBI connection; dbt needs R connections closed while it runs."
    )
  }
  if (!inherits(input, "Id")) {
    dataraft.core::dr_internal_scalar(input, "input staging table")
  }
  dataraft.core::dr_internal_flag(full_refresh, "full_refresh")
  dataraft.core::dr_internal_flag(echo, "echo")
  structure(
    list(
      project = project,
      model = model,
      connection = connection,
      input = input,
      full_refresh = full_refresh,
      vars = vars,
      echo = echo,
      timeout = timeout
    ),
    class = "dr_dbt_transform"
  )
}


#' @export
#' @importFrom dataraft.core dr_check_component
dr_check_component.dr_dbt_transform <- function(x, ...) {
  dataraft.core::dr_internal_need("processx")
  if (!file.exists(file.path(x$project$path, "dbt_project.yml"))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "The dbt project is missing dbt_project.yml. Check dr_transform_dbt(project = ...)."
    )
  }
  executable <- x$project$executable
  if (!nzchar(Sys.which(executable)) && !file.exists(executable)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "Install dbt and set dr_dbt_project(executable = ...) before running this transform."
    )
  }
  invisible(x)
}


#' @export
#' @importFrom dataraft.core dr_inspect
dr_inspect.dr_dbt_transform <- function(x, ...) {
  list(
    type = "dbt transformation",
    model = x$model,
    input = if (inherits(x$input, "Id")) as.list(x$input@name) else x$input,
    project = x$project$path,
    connection = "factory"
  )
}


#' @export
#' @importFrom dataraft.core dr_execute_transform
dr_execute_transform.dr_dbt_transform <- function(transform, data, ...) {
  dataraft.core::dr_check_component(transform)
  data <- dataraft.core::dr_collect(data)
  with_dbt_connection(transform$connection, function(con) {
    DBI::dbWithTransaction(con, {
      DBI::dbWriteTable(con, transform$input, data, overwrite = TRUE)
    })
  })
  # A package-qualified selector excludes identically named dependency models.
  parts <- strsplit(transform$model, ".", fixed = TRUE)[[1]]
  selector <- paste0(
    "package:",
    parts[[2]],
    ",resource_type:model,fqn:",
    parts[[3]]
  )
  result <- dr_dbt_build(
    transform$project,
    select = selector,
    full_refresh = transform$full_refresh,
    vars = transform$vars,
    echo = transform$echo,
    timeout = transform$timeout
  )
  node <- result$manifest$nodes[[transform$model]]
  succeeded <- result$results$unique_id[result$results$status == "success"]
  if (
    is.null(node) ||
      !identical(node$resource_type, "model") ||
      identical(node$config$materialized, "ephemeral") ||
      !transform$model %in% succeeded
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "The requested materialized dbt model did not succeed in this build.",
      "dr_dbt_invalid",
      result = result
    )
  }
  relation <- DBI::Id(
    catalog = dataraft.core::dr_internal_scalar(node$database, "dbt database"),
    schema = dataraft.core::dr_internal_scalar(node$schema, "dbt schema"),
    table = dataraft.core::dr_internal_scalar(
      node$alias %||% node$name,
      "dbt relation"
    )
  )
  output <- with_dbt_connection(transform$connection, function(con) {
    tibble::as_tibble(DBI::dbReadTable(con, relation))
  })
  attr(output, "dr_transform_metadata") <- list(
    engine = "dbt",
    model = transform$model,
    invocation_id = result$manifest$metadata$invocation_id,
    artifacts_dir = result$artifacts_dir
  )
  output
}


with_dbt_connection <- function(factory, fn) {
  rlang::local_error_call(rlang::caller_env())
  con <- factory()
  if (!inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_dbt",
      "The dbt connection factory must return a valid new DBI connection."
    )
  }
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  fn(con)
}


#' @export
#' @importFrom dataraft.core dr_capabilities
dr_capabilities.dr_dbt_transform <- function(x, ...) {
  dataraft.core::dr_component_capabilities(
    read = FALSE,
    write = TRUE,
    lazy = FALSE,
    transactions = FALSE,

    immutable = FALSE
  )
}
