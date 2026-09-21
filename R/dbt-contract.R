#' Export an R contract as dbt model properties
#'
#' Create an ordinary YAML-ready list containing explicit column types,
#' not-null tests and a single-column unique-key test. The default SQL types
#' target dbt-duckdb. Other adapters can supply explicit `types` overrides.
#' This does not install dbt packages, modify a project or execute validation.
#'
#' Schema enforcement is supported for SQL models with compatible dbt
#' materializations. It enforces the exact exported names and types. Required
#' fields and keys use dbt data tests, whose execution is the responsibility of
#' `dbt build` or `dbt test`; no database constraint guarantee is implied.
#'
#' R functions, formulas, pointblank rules and composite-key checks are not
#' translated. By default they cause an error. With `unsupported = "report"`,
#' a warning names them and `config.meta.dataraft.untranslated` records them
#' in the exported model. Missing SQL types always cause an error. R lifecycle
#' policies such as empty-data, freshness and additional-column handling are
#' recorded separately under `r_policies`; they still require R validation.
#' The export is deliberately a bounded schema bridge, not a replacement for
#' the original contract or its quality engines.
#'
#' @param contract A [dataraft.core::dr_contract()] specification.
#' @param name dbt model identifier.
#' @param enforced Whether dbt should enforce the exact model schema.
#' @param types Optional named character vector overriding SQL types by column.
#'   For example, `c(amount = "decimal(18,2)")`. Nested R list columns require
#'   an explicit SQL type because their shape cannot be inferred from `list`.
#' @param unsupported How to handle checks that cannot be translated:
#'   `"error"` (default) or `"report"` with a warning and metadata.
#' @returns A regular list with `version = 2` and one `models` entry, suitable
#'   for `yaml::write_yaml()` or composition into dbt model properties.
#' @seealso [dr_dbt_sources()], [dr_dbt_init()]
#' @examples
#' schema <- dataraft.core::dr_contract(columns = c(id = "integer", amount = "numeric"), key = "id")
#' properties <- dr_dbt_contract(schema, "orders")
#' properties$models[[1]]$columns
#' @export
dr_dbt_contract <- function(
  contract,
  name,
  enforced = TRUE,
  types = NULL,
  unsupported = c("error", "report")
) {
  if (!inherits(contract, "dr_contract")) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "Use dr_contract() to define the R schema first.",
      "dr_dbt_invalid"
    )
  }
  dataraft.core::ident(name)
  dataraft.core::flag(enforced, "enforced")
  if (enforced) {
    dataraft.core::assert_contract_ready(contract)
  }
  unsupported <- match.arg(unsupported)
  columns <- unlist(contract$columns, use.names = TRUE)
  if (
    !is.character(columns) ||
      !length(columns) ||
      is.null(names(columns)) ||
      anyNA(columns) ||
      anyNA(names(columns)) ||
      any(!nzchar(names(columns))) ||
      anyDuplicated(names(columns))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "The contract contains an invalid column schema.",
      "dr_dbt_invalid"
    )
  }
  sql_types <- dbt_sql_types(columns, types)
  untranslated <- character()
  if (length(contract$rules)) {
    untranslated <- c(
      untranslated,
      vapply(
        contract$rules,
        function(rule) {
          paste0(
            "quality rule: ",
            rule$name,
            " (",
            rule$engine %||% "custom",
            ")"
          )
        },
        character(1)
      )
    )
  }
  if (length(contract$key) > 1L) {
    untranslated <- c(
      untranslated,
      paste0(
        "composite key: ",
        paste(contract$key, collapse = ", ")
      )
    )
  } else if (
    length(contract$key) == 1L &&
      !contract$key %in% contract$required
  ) {
    untranslated <- c(
      untranslated,
      paste0(
        "nullable unique key: ",
        contract$key,
        " (dbt unique ignores NULL values)"
      )
    )
  }
  if (length(untranslated)) {
    message <- paste0(
      "These contract checks need R validation or explicit dbt tests: ",
      paste(untranslated, collapse = "; "),
      "."
    )
    if (unsupported == "error") {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        message,
        "dr_dbt_contract_untranslated",
        untranslated = untranslated
      )
    }
    rlang::warn(
      message,
      class = "dr_dbt_contract_untranslated",
      untranslated = untranslated
    )
  }
  properties <- lapply(names(columns), function(column) {
    value <- list(name = column, data_type = unname(sql_types[[column]]))
    description <- contract$column_metadata[[column]]$description
    if (!is.null(description)) {
      value$description <- description
    }
    tests <- character()
    if (column %in% contract$required) {
      tests <- c(tests, "not_null")
    }
    if (length(contract$key) == 1L && column %in% contract$key) {
      tests <- c(tests, "unique")
    }
    if (length(tests)) {
      value$data_tests <- as.list(tests)
    }
    value
  })
  metadata <- list(
    contract_id = contract$id,
    contract_version = contract$version,
    owner = contract$owner,
    r_policies = list(
      allow_empty = contract$allow_empty,
      allow_extra = contract$allow_extra,
      max_age_hours = contract$max_age_hours
    )
  )
  if (length(untranslated)) {
    metadata$untranslated <- as.list(untranslated)
  }
  model <- list(
    name = name,
    description = contract$description,
    config = list(
      contract = list(enforced = enforced, alias_types = FALSE),
      meta = list(dataraft = metadata)
    ),
    columns = properties
  )
  list(version = 2L, models = list(model))
}


dbt_sql_types <- function(columns, overrides = NULL) {
  rlang::local_error_call(rlang::caller_env())
  mapping <- c(
    character = "varchar",
    integer = "integer",
    numeric = "double",
    logical = "boolean",
    Date = "date",
    POSIXct = "timestamptz",
    integer64 = "bigint"
  )
  result <- stats::setNames(unname(mapping[columns]), names(columns))
  if (!is.null(overrides)) {
    if (
      !is.character(overrides) ||
        !length(overrides) ||
        is.null(names(overrides)) ||
        anyNA(overrides) ||
        anyNA(names(overrides)) ||
        any(!nzchar(names(overrides))) ||
        anyDuplicated(names(overrides)) ||
        !all(names(overrides) %in% names(columns)) ||
        any(!grepl("^[A-Za-z][A-Za-z0-9_ (),\\[\\]]*$", overrides, perl = TRUE))
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "types must name declared columns and contain explicit SQL types.",
        "dr_dbt_invalid"
      )
    }
    result[names(overrides)] <- overrides
  }
  if (anyNA(result)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      paste0(
        "Supply explicit SQL types for: ",
        paste(names(result)[is.na(result)], collapse = ", "),
        "."
      ),
      "dr_dbt_invalid"
    )
  }
  result
}
