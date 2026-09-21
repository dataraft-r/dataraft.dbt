#' Describe a dbt project for execution from R
#'
#' Store paths and CLI settings without running dbt or opening a database.
#' dbt remains responsible for SQL models, dependencies, tests and incremental
#' strategies. Use [dr_dbt_build()] or [dataraft.core::dr_run()] to execute the project.
#'
#' @param path Character scalar giving the directory with `dbt_project.yml`.
#'   The directory is checked when executing, not when constructing the object.
#' @param profiles_dir Character scalar pointing to a directory containing
#'   `profiles.yml`, or `NULL` to use dbt's own profile discovery.
#' @param target Optional character scalar naming a target in the dbt profile.
#' @param executable Character scalar giving a dbt executable name or path.
#'   It is passed to [processx::run()] without a shell.
#' @param lake Optional connection-free [dataraft.lake::dr_lake_config()] for local DuckDB or
#'   DuckLake. At execution, dataraft writes a private profile for this catalog.
#'   Omit `profiles_dir` when using `lake`. SQL and handwritten project files
#'   stay unchanged. Close any caller-owned connections before running dbt.
#' @param sources Optional named list of successful published lake results.
#'   They become the `inputs` source group. Use [dr_dbt_sources()] to add groups
#'   from other physical schemas. Definitions retain exact release IDs;
#'   registry resolution and YAML generation happen only at execution.
#' @returns A serializable `dbt_project` specification. It contains no
#'   database connection or resolved environment credentials.
#' @seealso [dr_dbt_init()], [dr_dbt_status()], [dr_dbt_model()]
#' @examples
#' project <- dr_dbt_project("analytics", profiles_dir = "analytics")
#' project
#' @export
dr_dbt_project <- function(
  path = "dbt",
  profiles_dir = NULL,
  target = NULL,
  executable = "dbt",
  lake = NULL,
  sources = NULL
) {
  dataraft.core::scalar(executable, "executable")
  if (!is.null(target)) {
    dataraft.core::scalar(target, "target")
  }
  if (!is.null(lake)) {
    dbt_managed_config(lake)
    if (!is.null(profiles_dir)) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "Choose lake for a managed profile or profiles_dir for an external profile, not both.",
        "dr_dbt_invalid"
      )
    }
  }
  project <- structure(
    list(
      path = dataraft.core::absolute_path(path),
      profiles_dir = if (is.null(profiles_dir)) {
        NULL
      } else {
        dataraft.core::absolute_path(profiles_dir)
      },
      target = target,
      executable = executable,
      lake = lake,
      source_groups = list()
    ),
    class = "dr_dbt_project"
  )
  if (!is.null(sources)) {
    if (is.null(lake)) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "Constructor sources require lake. For an external project use dr_dbt_sources() explicitly.",
        "dr_dbt_invalid"
      )
    }
    project <- dr_dbt_sources(project, sources, name = "inputs")
  }
  project
}


#' Build or test SQL models with dbt
#'
#' Run the dbt CLI in a separate process and read that invocation's artifacts.
#' Each invocation gets a new directory under `.dataraft/runs` in the project;
#' a failed invocation can never reuse an earlier run's successful results.
#'
#' Close R connections to the same local DuckDB or DuckLake catalog before
#' running dbt, then reconnect for analysis. dbt manages its own connections.
#' Anonymous usage telemetry is disabled for the child process.
#' A dbt build is not an atomic dataraft release: earlier models can have been
#' materialized even if a later model or test fails. dbt logs and artifacts can
#' contain SQL, paths and database messages; treat them as project data.
#'
#' @param project A [dr_dbt_project()] specification.
#' @param select,exclude Optional character vectors of dbt selection
#'   expressions.
#'   Each element is one CLI argument; expressions starting with `-` are
#'   rejected.
#' @param full_refresh Logical scalar. Rebuild incremental models when `TRUE`.
#' @param vars A named list of non-secret dbt variables, encoded as JSON.
#' @param echo Logical scalar. Stream dbt output to the R console.
#' @param timeout Positive timeout in seconds, or `Inf` for no limit.
#' @param stop_on_failure Logical scalar. Raise `dr_dbt_failed` on a nonzero
#'   exit
#'   status, failed nodes or missing artifacts. The condition's `result` field
#'   retains diagnostics. Set `FALSE` to inspect failures as ordinary results.
#' @param catalog Optional [dataraft.catalog::dr_catalog_openmetadata_dbt()] adapter, a function
#'   receiving the dbt result, or an S3 catalog whose `dataraft.core::dr_capabilities()` declares
#'   `metadata_inputs = "dr_dbt_result"` and implements `dataraft.core::dr_publish_metadata()`.
#'   After dbt
#'   finishes, hand this invocation's artifacts to OpenMetadata's ingestion
#'   engine. A delivery failure warns and leaves the dbt outcome unchanged.
#'   Retry with `dataraft.core::dr_publish_metadata(catalog, result)` without rebuilding models.
#' @returns A `dr_dbt_result` list with `status` (integer exit code), `success`
#'   (logical), `command`, `results` (node tibble), parsed `manifest`,
#'   `artifacts_dir`, `stdout`, `stderr`, `artifact_error`, `invocation_id`,
#'   `artifact_hashes`, original `project`, resolved `source_bindings`,
#'   `source_catalog` and optional `catalog_delivery`. Warnings reported
#'   by dbt are retained and do not by themselves count as failure.
#' @seealso [dr_dbt_status()], [dr_dbt_lineage()], [dr_dbt_model()]
#' @examplesIf nzchar(Sys.getenv("DATARAFT_DBT_EXAMPLE_PROJECT"))
#' project <- dr_dbt_project(Sys.getenv("DATARAFT_DBT_EXAMPLE_PROJECT"))
#' result <- dr_dbt_build(project, select = "tag:reporting", echo = FALSE)
#' dr_dbt_status(result)
#' @export
dr_dbt_build <- function(
  project,
  select = NULL,
  exclude = NULL,
  full_refresh = FALSE,
  vars = list(),
  echo = TRUE,
  timeout = Inf,
  stop_on_failure = TRUE,
  catalog = NULL
) {
  dbt_run(
    project,
    "build",
    select,
    exclude,
    full_refresh,
    vars,
    echo,
    timeout,
    stop_on_failure,
    catalog
  )
}


#' @rdname dr_dbt_build
#' @export
dr_dbt_test <- function(
  project,
  select = NULL,
  exclude = NULL,
  vars = list(),
  echo = TRUE,
  timeout = Inf,
  stop_on_failure = TRUE,
  catalog = NULL
) {
  dbt_run(
    project,
    "test",
    select,
    exclude,
    FALSE,
    vars,
    echo,
    timeout,
    stop_on_failure,
    catalog
  )
}


#' @export
#' @noRd
#' @importFrom dataraft.core dr_execute
dr_execute.dr_dbt_project <- function(
  object,
  lake = NULL,
  execution = NULL,
  sources = NULL,
  ...
) {
  if (!is.null(execution)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "Execution defaults apply to R products. Configure dbt through its project."
    )
  }
  if (!is.null(lake)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "dbt opens its own connections; omit lake.",
      "dr_dbt_invalid"
    )
  }
  object <- dataraft.core::replace_execution_sources(object, sources = sources)
  dr_dbt_build(object, ...)
}


dbt_selection <- function(value, name) {
  rlang::local_error_call(rlang::caller_env())
  if (is.null(value)) {
    return(character())
  }
  if (
    !is.character(value) ||
      !length(value) ||
      anyNA(value) ||
      any(!nzchar(trimws(value))) ||
      any(startsWith(trimws(value), "-"))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      paste(name, "must contain non-empty dbt selectors, not CLI flags."),
      "dr_dbt_invalid"
    )
  }
  c(paste0("--", name), value)
}


dbt_process <- function(command, args, wd, echo, timeout) {
  rlang::local_error_call(rlang::caller_env())
  processx::run(
    command,
    args,
    wd = wd,
    echo = echo,
    timeout = timeout,
    error_on_status = FALSE,
    cleanup_tree = TRUE,
    env = c(
      "current",
      DBT_SEND_ANONYMOUS_USAGE_STATS = "false",
      DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS = "false",
      DO_NOT_TRACK = "1"
    )
  )
}


dbt_run <- function(
  project,
  command,
  select,
  exclude,
  full_refresh,
  vars,
  echo,
  timeout,
  stop_on_failure,
  catalog = NULL
) {
  rlang::local_error_call(rlang::caller_env())
  if (!inherits(project, "dr_dbt_project")) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "Use dr_dbt_project() first.",
      "dr_dbt_invalid"
    )
  }
  dataraft.core::flag(full_refresh, "full_refresh")
  dataraft.core::flag(echo, "echo")
  dataraft.core::flag(stop_on_failure, "stop_on_failure")
  dbt_check_catalog(catalog)
  if (
    !is.numeric(timeout) ||
      length(timeout) != 1L ||
      is.na(timeout) ||
      timeout <= 0
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "timeout must be a positive number of seconds or Inf.",
      "dr_dbt_invalid"
    )
  }
  if (
    !is.list(vars) ||
      (length(vars) &&
        (is.null(names(vars)) ||
          anyNA(names(vars)) ||
          any(!nzchar(names(vars))) ||
          anyDuplicated(names(vars))))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "vars must be a named list with unique non-empty names.",
      "dr_dbt_invalid"
    )
  }
  selectors <- c(
    dbt_selection(select, "select"),
    dbt_selection(exclude, "exclude")
  )
  if (!file.exists(file.path(project$path, "dbt_project.yml"))) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "No dbt_project.yml found. Use dr_dbt_init() or supply an existing project.",
      "dr_dbt_invalid"
    )
  }
  dataraft.core::need("processx")
  executable <- Sys.which(project$executable)
  if (!nzchar(executable) && !file.exists(project$executable)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "dbt executable not found. Install dbt and set executable to its path.",
      "dr_dbt_unavailable"
    )
  }
  executable <- if (nzchar(executable)) {
    unname(executable)
  } else {
    dataraft.core::absolute_path(project$executable)
  }
  prepared <- dbt_prepare_managed(project)
  if (!is.null(prepared) && !length(project$source_groups)) {
    # Initialize even a source-free project through the lake lifecycle. dbt must
    # not create an unmarked catalog that publication later cannot recognize.
    owned <- dataraft.lake::dr_connect_lake(project$lake)
    dataraft.lake::dr_close_lake(owned)
  }
  artifacts <- file.path(
    project$path,
    ".dataraft",
    "runs",
    dataraft.core::uid()
  )
  if (!dir.create(artifacts, recursive = TRUE)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "Could not create the dbt artifact directory.",
      "dr_dbt_io"
    )
  }
  profiles_dir <- project$profiles_dir
  target <- project$target
  if (!is.null(prepared)) {
    profiles_dir <- dbt_write_managed(prepared, artifacts)
    target <- prepared$target
  }
  args <- c(
    command,
    "--project-dir",
    project$path,
    "--target-path",
    artifacts,
    "--log-path",
    file.path(artifacts, "logs"),
    selectors
  )
  if (!is.null(profiles_dir)) {
    args <- c(args, "--profiles-dir", profiles_dir)
  }
  if (!is.null(prepared)) {
    args <- c(args, "--profile", prepared$profile)
  }
  if (!is.null(target)) {
    args <- c(args, "--target", target)
  }
  if (full_refresh) {
    args <- c(args, "--full-refresh")
  }
  if (length(vars)) {
    args <- c(
      args,
      "--vars",
      as.character(jsonlite::toJSON(vars, auto_unbox = TRUE))
    )
  }
  process <- tryCatch(
    dbt_process(executable, args, project$path, echo, timeout),
    error = function(e) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "dbt could not complete. Check the executable, timeout and project logs.",
        "dr_dbt_process_error",
        parent = e,
        artifacts_dir = artifacts
      )
    }
  )
  result <- structure(
    list(
      project = project,
      source_bindings = prepared$source_bindings %||% list(),
      source_catalog = prepared$catalog,
      command = command,
      status = as.integer(process$status),
      success = FALSE,
      results = dbt_empty_results(),
      manifest = NULL,
      artifacts_dir = artifacts,
      stdout = process$stdout,
      stderr = process$stderr,
      artifact_error = NULL,
      invocation_id = NULL,
      artifact_hashes = NULL,
      catalog_delivery = NULL
    ),
    class = "dr_dbt_result"
  )
  parsed <- tryCatch(dbt_read_artifacts(artifacts), error = identity)
  if (inherits(parsed, "error")) {
    result$artifact_error <- conditionMessage(parsed)
  } else {
    result$results <- parsed$results
    result$manifest <- parsed$manifest
    result$invocation_id <- parsed$manifest$metadata$invocation_id
    result$artifact_hashes <- dbt_artifact_hashes(artifacts)
    result$success <- identical(result$status, 0L) &&
      all(result$results$status %in% c("success", "pass", "warn"))
  }
  if (!is.null(catalog)) {
    result$catalog_delivery <- dbt_deliver_metadata(catalog, result)
  }
  if (stop_on_failure && !result$success) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "dbt execution failed. Inspect condition$result or use stop_on_failure = FALSE.",
      "dr_dbt_failed",
      result = result
    )
  }
  result
}


dbt_check_catalog <- function(catalog) {
  rlang::local_error_call(rlang::caller_env())
  if (is.null(catalog) || is.function(catalog)) {
    return(invisible(catalog))
  }
  if (
    !dataraft.core::component_method("dr_publish_metadata", catalog) ||
      !"dr_dbt_result" %in%
        dataraft.core::dr_capabilities(catalog)$metadata_inputs
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "catalog must be a function or a dr_publish_metadata() adapter declaring metadata_inputs = 'dr_dbt_result' in dr_capabilities().",
      "dr_dbt_invalid"
    )
  }
  invisible(catalog)
}


dbt_deliver_metadata <- function(catalog, result) {
  rlang::local_error_call(rlang::caller_env())
  delivery <- list(
    status = "pending",
    destination = if (is.function(catalog)) {
      "callback"
    } else {
      class(catalog)[[1L]]
    },
    invocation_id = result$invocation_id,
    attempt = 1L,
    started_at = dataraft.core::now(),
    finished_at = NULL,
    exit_status = NULL,
    error_class = NULL,
    message = NULL,
    recorded = FALSE
  )
  valid <- tryCatch(dbt_catalog_artifacts(result), error = identity)
  if (inherits(valid, "error")) {
    delivery$status <- "blocked"
    delivery$error_class <- "dr_dbt_artifact_invalid"
    delivery$message <- "Metadata delivery blocked: dbt artifacts are missing, malformed or changed. Use the original unchanged artifacts from this invocation."
  } else {
    outcome <- tryCatch(
      dataraft.core::dr_publish_metadata(catalog, result),
      error = identity
    )
    if (inherits(outcome, "error")) {
      delivery$error_class <- "dr_dbt_catalog_delivery_error"
      delivery$message <- "Metadata delivery failed. Check the catalog configuration and connectivity, then retry dr_publish_metadata(catalog, result)."
    } else if (
      !is.function(catalog) &&
        is.list(outcome) &&
        is.character(outcome$status) &&
        length(outcome$status) == 1L &&
        outcome$status %in% c("delivered", "pending", "blocked")
    ) {
      # Structured adapter receipts are part of its public delivery contract.
      # Do not retain arbitrary adapter return values or callback environments.
      return(outcome[intersect(names(delivery), names(outcome))])
    } else {
      delivery$status <- "delivered"
    }
  }
  delivery$finished_at <- dataraft.core::now()
  if (delivery$status != "delivered") {
    dbt_catalog_warning(delivery)
  }
  delivery
}


dbt_empty_results <- function() {
  rlang::local_error_call(rlang::caller_env())
  tibble::tibble(
    unique_id = character(),
    status = character(),
    execution_time = double(),
    failures = integer(),
    message = character()
  )
}


dbt_read_json <- function(path) {
  rlang::local_error_call(rlang::caller_env())
  if (!file.exists(path)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      paste("Missing dbt artifact:", basename(path)),
      "dr_dbt_artifact_invalid"
    )
  }
  tryCatch(
    jsonlite::read_json(path, simplifyVector = FALSE),
    error = function(e) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        paste("Invalid dbt JSON:", basename(path)),
        "dr_dbt_artifact_invalid"
      )
    }
  )
}


dbt_read_artifacts <- function(path) {
  rlang::local_error_call(rlang::caller_env())
  runs <- dbt_read_json(file.path(path, "run_results.json"))
  manifest <- dbt_read_manifest(path)
  if (!is.list(runs) || is.null(runs$metadata) || !is.list(runs$results)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "run_results.json must contain metadata and a results array.",
      "dr_dbt_artifact_invalid"
    )
  }
  run_id <- runs$metadata$invocation_id
  manifest_id <- manifest$metadata$invocation_id
  if (
    !is.character(run_id) ||
      length(run_id) != 1L ||
      is.na(run_id) ||
      !nzchar(run_id) ||
      !identical(run_id, manifest_id)
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "dbt artifacts must belong to the same invocation.",
      "dr_dbt_artifact_invalid"
    )
  }
  rows <- lapply(runs$results, function(node) {
    if (
      !is.list(node) ||
        !is.character(node$unique_id) ||
        length(node$unique_id) != 1L ||
        !is.character(node$status) ||
        length(node$status) != 1L
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "A dbt result is missing unique_id or status.",
        "dr_dbt_artifact_invalid"
      )
    }
    for (field in c("execution_time", "failures", "message")) {
      value <- node[[field]]
      valid <- is.null(value) ||
        (length(value) == 1L &&
          if (field == "message") is.character(value) else is.numeric(value))
      if (!valid) {
        dataraft.core::abort(
          subclass = "dataraft_error_dbt",
          paste("Invalid scalar dbt result field:", field),
          "dr_dbt_artifact_invalid"
        )
      }
    }
    tibble::tibble(
      unique_id = node$unique_id,
      status = node$status,
      execution_time = as.double(node$execution_time %||% NA_real_),
      failures = as.integer(node$failures %||% NA_integer_),
      message = as.character(node$message %||% NA_character_)
    )
  })
  list(
    results = if (length(rows)) dplyr::bind_rows(rows) else dbt_empty_results(),
    manifest = manifest
  )
}


dbt_read_manifest <- function(path) {
  rlang::local_error_call(rlang::caller_env())
  manifest <- dbt_read_json(file.path(path, "manifest.json"))
  if (
    !is.list(manifest) ||
      !is.list(manifest$nodes) ||
      !is.list(manifest$metadata)
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "manifest.json must contain metadata and nodes.",
      "dr_dbt_artifact_invalid"
    )
  }
  manifest
}


#' Inspect the node results of a dbt invocation
#'
#' Read structured statuses without parsing console output. A directory must
#' contain a matching pair of `run_results.json` and `manifest.json` artifacts.
#' The returned table contains executed nodes only, not the entire dbt project.
#' @param x A `dr_dbt_result`, or a character scalar giving an artifact
#'   directory.
#' @returns A tibble with character columns `unique_id`, `status`, `message`,
#'   numeric `execution_time` (seconds) and integer `failures` (possibly `NA`).
#' @seealso [dr_dbt_build()], [dr_dbt_lineage()]
#' @examples
#' artifacts <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
#' dr_dbt_status(artifacts)
#' @export
dr_dbt_status <- function(x) {
  if (inherits(x, "dr_dbt_result")) {
    return(x$results)
  }
  dbt_read_artifacts(dataraft.core::absolute_path(x))$results
}


#' Extract dependency edges from a dbt manifest
#'
#' Return declared dependencies for models, tests, sources, exposures and other
#' manifest nodes. Dependencies describe SQL builds, not primary/foreign keys
#' or column-level lineage. No database connection or dbt installation is
#'   needed.
#' @inheritParams dr_dbt_status
#' @returns A tibble with character columns `from`, `to` and `resource_type`.
#'   One row represents a dependency from a parent to a downstream resource.
#' @seealso [dr_dbt_model()], [dr_dbt_status()]
#' @examples
#' artifacts <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
#' dr_dbt_lineage(artifacts)
#' @export
dr_dbt_lineage <- function(x) {
  manifest <- dbt_manifest(x)
  nodes <- c(
    manifest$nodes,
    manifest$sources,
    manifest$exposures,
    manifest$metrics,
    manifest$semantic_models,
    manifest$saved_queries
  )
  rows <- lapply(names(nodes), function(id) {
    parents <- unlist(nodes[[id]]$depends_on$nodes, use.names = FALSE)
    if (!length(parents)) {
      return(NULL)
    }
    tibble::tibble(
      from = as.character(parents),
      to = id,
      resource_type = nodes[[id]]$resource_type %||% "unknown"
    )
  })
  out <- dplyr::bind_rows(rows)
  if (!nrow(out)) {
    return(tibble::tibble(
      from = character(),
      to = character(),
      resource_type = character()
    ))
  }
  unique(out)
}


dbt_manifest <- function(x) {
  rlang::local_error_call(rlang::caller_env())
  if (inherits(x, "dr_dbt_result")) {
    if (is.null(x$manifest)) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "This dbt result has no valid manifest.",
        "dr_dbt_artifact_invalid"
      )
    }
    return(x$manifest)
  }
  dbt_read_manifest(dataraft.core::absolute_path(x))
}


#' Open materialized dbt relations as a lazy relational model
#'
#' Build a [dm::dm()] from explicit relation names in a dbt manifest. The lake
#' must refer to the same catalog used by the dbt project. SQL dependencies do
#' not imply relational keys; declare keys explicitly when needed.
#'
#' Relations are current dbt tables or views, not immutable dataraft releases.
#' The returned lazy model borrows the supplied connection; keep it open while
#' querying. Ephemeral models cannot be opened. This function does not certify
#' that tables were successfully built or that they still match the artifacts.
#' @param lake A connected lake from [dataraft.lake::dr_connect_lake()].
#' @inheritParams dr_dbt_status
#' @param tables Optional named character vector mapping R table aliases to dbt
#'   unique IDs, such as `c(sales = "model.shop.sales")`. By default includes
#'   all
#'   non-ephemeral models, seeds and snapshots. Use a subset after selected
#'   builds.
#' @param database Character scalar: the attachment name in the R connection.
#'   Defaults to `"lake"`, as created by [dataraft.lake::dr_connect_lake()].
#' @inheritParams dataraft.core::dr_model
#' @returns A lazy `dm` object with a `dr_dbt_nodes` attribute mapping aliases
#'   to dbt unique IDs. Key violations raise `dr_model_invalid` when `check` is
#'   `TRUE`; SQL and connection errors are propagated.
#' @seealso [dataraft.core::dr_model()] for immutable releases, [dr_dbt_lineage()]
#' @examplesIf nzchar(Sys.getenv("DATARAFT_DBT_EXAMPLE_PROJECT"))
#' # See vignette("dbt-workflows") for a complete build and reconnect example.
#' project <- dr_dbt_project(Sys.getenv("DATARAFT_DBT_EXAMPLE_PROJECT"))
#' project
#' @export
dr_dbt_model <- function(
  lake,
  x,
  tables = NULL,
  database = "lake",
  primary_keys = list(),
  foreign_keys = list(),
  check = TRUE
) {
  dataraft.lake::assert_lake(lake)
  dataraft.core::need("dm")
  dataraft.core::scalar(database, "database")
  dataraft.core::flag(check, "check")
  manifest <- dbt_manifest(x)
  available <- Filter(
    function(node) {
      node$resource_type %in%
        c("model", "seed", "snapshot") &&
        !identical(node$config$materialized, "ephemeral")
    },
    manifest$nodes
  )
  if (is.null(tables)) {
    tables <- stats::setNames(
      names(available),
      vapply(available, function(node) node$alias %||% node$name, character(1))
    )
  }
  if (
    !is.character(tables) ||
      !length(tables) ||
      anyNA(tables) ||
      is.null(names(tables)) ||
      anyNA(names(tables)) ||
      any(!nzchar(names(tables))) ||
      anyDuplicated(names(tables)) ||
      !all(tables %in% names(available))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "tables must map unique R aliases to materialized dbt node IDs.",
      "dr_dbt_invalid"
    )
  }
  relations <- lapply(tables, function(id) {
    node <- available[[id]]
    schema <- dataraft.core::scalar(node$schema, "dbt schema")
    table <- dataraft.core::scalar(
      node$alias %||% node$name,
      "dbt relation name"
    )
    dplyr::tbl(
      lake$con,
      DBI::Id(catalog = database, schema = schema, table = table)
    )
  })
  model <- dataraft.core::dm_keys(
    dm::dm(!!!relations),
    primary_keys,
    foreign_keys,
    check
  )
  attr(model, "dr_dbt_nodes") <- tables
  model
}


#' @export
print.dr_dbt_project <- function(x, ...) {
  cat(
    "<dbt_project>\nProject:",
    x$path,
    "\nProfile:",
    if (is.null(x$lake)) "external" else paste("managed", x$lake$backend),
    "\nSource groups:",
    length(x$source_groups %||% list()),
    "\nTarget:",
    x$target %||% if (is.null(x$lake)) "profile default" else "dataraft",
    "\n"
  )
  invisible(x)
}

#' @export
print.dr_dbt_result <- function(x, ...) {
  cat(
    "<dr_dbt_result>",
    x$command,
    "| exit:",
    x$status,
    "|",
    if (x$success) "success" else "failed",
    "\n"
  )
  print(x$results)
  if (!is.null(x$artifact_error)) {
    cat("Artifacts:", x$artifact_error, "\n")
  }
  invisible(x)
}
