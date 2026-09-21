#' Create a runnable local dbt starter project
#'
#' Write a small dbt-duckdb project with RAW inputs, staging casts, a core
#' order model, customer revenue and uniqueness/not-null tests. Supply accepted
#' R-ingested `sources` to use real immutable RAW relations. With no sources,
#' the starter remains a runnable synthetic seed demonstration. Existing
#' non-empty directories are never overwritten. No connection is opened.
#'
#' The generated profile targets the catalog in `config` through the `lake`
#' attachment. Supported configurations use a local metadata catalog and local
#' storage. For S3, PostgreSQL or dbt v2 catalog configuration, create and
#'   verify
#' an appropriate profile yourself and use [dr_dbt_project()]. The starter
#' uses the dbt-duckdb profile format; it does not install dbt or adapters.
#'
#' @param path Character scalar giving a new or empty directory.
#' @param config A [dataraft.lake::dr_lake_config()] with a local DuckDB catalog and local storage.
#' @param name Character scalar. A dbt project identifier containing letters,
#'   digits and underscores, starting with a letter.
#' @param sources Optional named list accepted by [dr_dbt_sources()]. The canned
#'   order example requires an `orders` entry whose recorded schema includes
#'   `order_id`, `customer_id` and `amount`. Staging casts `amount` to SQL
#'   `double`, including when input amounts are character strings. Invalid
#'   numeric strings fail the dbt build. Other named sources also receive
#'   staging models. For arbitrary business models use [dr_dbt_sources()] with
#'   your own project instead of this order-specific starter.
#' @inheritParams dr_dbt_project
#' @returns A [dr_dbt_project()] specification pointing to the written project
#'   and profile. Its `source_config` records the expected catalog. Source mode
#'   creates no seeds and never writes RAW tables. Configure
#'   `layers = c("raw", "staging", "core", "marts")` in [dataraft.lake::dr_lake_config()].
#'   A named layer vector can map `staging`, `core` and `marts` to other schema
#'   names; the ingestion schema remains `raw`. Seed mode also works with the
#'   usual three-layer lake configuration, creating its dbt schemas on build.
#' @seealso [dr_dbt_build()], [dataraft.core::dr_product()]
#' @examplesIf requireNamespace("duckdb", quietly = TRUE) && requireNamespace("yaml", quietly = TRUE)
#' root <- tempfile("dataraft-example-")
#' config <- dataraft.lake::dr_lake_config(
#'   catalog = dataraft.lake::dr_registry_duckdb(file.path(root, "lake.duckdb")),
#'   storage = dataraft.lake::dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"
#' )
#' project <- dr_dbt_init(file.path(root, "dbt"), config)
#' project
#' unlink(root, recursive = TRUE)
#' @export
dr_dbt_init <- function(
  path,
  config,
  name = "dataraft_demo",
  executable = "dbt",
  sources = NULL
) {
  dataraft.core::need("yaml")
  dataraft.core::ident(name)
  if (
    !inherits(config, "dr_config") ||
      config$catalog$type != "duckdb" ||
      config$storage$type != "local"
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "The starter requires dr_lake_config() with local catalog and storage.",
      "dr_dbt_invalid"
    )
  }
  schemas <- dbt_starter_schemas(config, !is.null(sources))
  source_types <- if (is.null(sources)) {
    list(
      orders = c(
        order_id = "integer",
        customer_id = "integer",
        amount = "numeric"
      )
    )
  } else {
    binding <- dbt_source_binding(sources)
    if (!identical(binding$database, "lake")) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "The starter profile attaches RAW releases under database lake.",
        "dr_dbt_invalid"
      )
    }
    if (!"orders" %in% names(sources)) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "The order starter needs sources = list(orders = accepted_raw). Use dr_dbt_sources() for a general project.",
        "dr_dbt_invalid"
      )
    }
    types <- lapply(sources, function(result) {
      schema <- result$metadata$schema
      if (is.list(schema)) {
        schema <- unlist(schema, use.names = TRUE)
      }
      if (
        !is.character(schema) ||
          !length(schema) ||
          is.null(names(schema)) ||
          anyNA(schema) ||
          anyNA(names(schema)) ||
          any(!nzchar(names(schema))) ||
          anyDuplicated(names(schema))
      ) {
        dataraft.core::abort(
          subclass = "dataraft_error_dbt",
          "The starter needs recorded column types on every accepted RAW result.",
          "dr_dbt_invalid"
        )
      }
      if (any(grepl("\\{\\{|\\{%|\\{#", names(schema)))) {
        dataraft.core::abort(
          subclass = "dataraft_error_dbt",
          "Rename source columns containing dbt template delimiters before using the starter.",
          "dr_dbt_invalid"
        )
      }
      dbt_sql_types(schema)
      schema
    })
    if (
      !all(c("order_id", "customer_id", "amount") %in% names(types$orders)) ||
        !types$orders[["amount"]] %in%
          c("character", "integer", "numeric", "integer64")
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "The order starter requires order_id, customer_id and amount as numbers or numeric strings. Use dr_dbt_sources() for other schemas.",
        "dr_dbt_invalid"
      )
    }
    if (
      any(vapply(
        sources,
        function(result) {
          !identical(
            dbt_catalog_fingerprint(result$output_config),
            dbt_catalog_fingerprint(config)
          )
        },
        logical(1)
      ))
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_dbt",
        "Starter RAW sources must belong to config's catalog.",
        "dr_dbt_invalid"
      )
    }
    types$orders[["amount"]] <- "numeric"
    types
  }
  path <- dataraft.core::absolute_path(path)
  if (file.exists(path) && !dir.exists(path)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "path is a file.",
      "dr_dbt_invalid"
    )
  }
  if (
    dir.exists(path) && length(list.files(path, all.files = TRUE, no.. = TRUE))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "Choose a new or empty directory; existing files are never overwritten.",
      "dr_dbt_invalid"
    )
  }
  project <- dr_dbt_project(path, path, target = "dev", executable = executable)
  project$source_config <- config
  directories <- c("models/staging", "models/core", "models/marts", "macros")
  if (is.null(sources)) {
    directories <- c(directories, "seeds")
  }
  for (directory in directories) {
    dir.create(
      file.path(path, directory),
      recursive = TRUE,
      showWarnings = FALSE
    )
  }
  profile <- dbt_profile_output(config, schemas[["staging"]])
  yaml::write_yaml(
    stats::setNames(
      list(list(target = "dev", outputs = list(dev = profile))),
      name
    ),
    file.path(path, "profiles.yml")
  )
  models <- stats::setNames(
    list(list(
      `+materialized` = "table",
      staging = list(`+schema` = schemas[["staging"]]),
      core = list(`+schema` = schemas[["core"]]),
      marts = list(`+schema` = schemas[["marts"]])
    )),
    name
  )
  definition <- list(
    name = name,
    version = "1.0.0",
    `config-version` = 2L,
    profile = name,
    `model-paths` = list("models"),
    `macro-paths` = list("macros"),
    models = models
  )
  if (is.null(sources)) {
    definition$`seed-paths` <- list("seeds")
    definition$seeds <- stats::setNames(list(list(`+schema` = "raw")), name)
    writeLines(
      c("order_id,customer_id,amount", "1,101,25", "2,101,75", "3,102,50"),
      file.path(path, "seeds", "raw_orders.csv")
    )
  } else {
    definition$`seed-paths` <- list()
  }
  yaml::write_yaml(definition, file.path(path, "dbt_project.yml"))
  if (!is.null(sources)) {
    dr_dbt_sources(project, sources, name = "raw")
  }
  for (source_name in names(source_types)) {
    columns <- source_types[[source_name]]
    sql_types <- dbt_sql_types(columns)
    quoted <- paste0('"', gsub('"', '""', names(columns), fixed = TRUE), '"')
    projection <- paste0("  cast(", quoted, " as ", sql_types, ") as ", quoted)
    input <- if (is.null(sources)) {
      "{{ ref('raw_orders') }}"
    } else {
      paste0("{{ source('raw', '", source_name, "') }}")
    }
    writeLines(
      c("select", paste(projection, collapse = ",\n"), paste("from", input)),
      file.path(path, "models", "staging", paste0("stg_", source_name, ".sql"))
    )
  }
  writeLines(
    c(
      "select order_id, customer_id, amount as order_amount,",
      "  amount > 0 as is_positive_order",
      "from {{ ref('stg_orders') }}"
    ),
    file.path(path, "models", "core", "core_orders.sql")
  )
  writeLines(
    c(
      "select customer_id, sum(order_amount) as revenue",
      "from {{ ref('core_orders') }}",
      "group by customer_id"
    ),
    file.path(path, "models", "marts", "customer_revenue.sql")
  )
  yaml::write_yaml(
    list(
      version = 2L,
      models = list(
        list(
          name = "stg_orders",
          description = "One row per order, with explicit source types.",
          columns = list(
            list(name = "order_id", tests = list("unique", "not_null")),
            list(name = "customer_id", tests = list("not_null"))
          )
        ),
        list(
          name = "core_orders",
          description = "Order amounts and a reusable positive-order flag.",
          columns = list(list(
            name = "order_id",
            tests = list("unique", "not_null")
          ))
        ),
        list(
          name = "customer_revenue",
          description = "Revenue by customer.",
          columns = list(
            list(name = "customer_id", tests = list("unique", "not_null"))
          )
        )
      )
    ),
    file.path(path, "models", "schema.yml")
  )
  writeLines(
    c(
      "{% macro generate_schema_name(custom_schema_name, node) -%}",
      "  {{ custom_schema_name | trim if custom_schema_name else target.schema }}",
      "{%- endmacro %}"
    ),
    file.path(path, "macros", "generate_schema_name.sql")
  )
  writeLines(
    c(
      ".dataraft/",
      "target/",
      "logs/",
      "dbt_packages/",
      "profiles.yml",
      "*.duckdb",
      "*.wal"
    ),
    file.path(path, ".gitignore")
  )
  writeLines(
    c(
      paste0("# ", name),
      "",
      if (is.null(sources)) {
        "Synthetic seed demonstration: raw -> staging -> core -> marts."
      } else {
        "Accepted R-ingested RAW releases -> staging -> core -> marts."
      },
      "",
      "Install a compatible dbt CLI and DuckDB adapter, then run from R:",
      "",
      "```r",
      "library(dataraft)",
      "project <- dr_dbt_project(\".\", profiles_dir = \".\")",
      "result <- dr_dbt_build(project)",
      "dr_dbt_status(result)",
      "dr_dbt_lineage(result)",
      "```",
      "",
      if (is.null(sources)) {
        "For real ingestion, create a new project with dr_dbt_init(..., sources = list(orders = accepted_raw))."
      } else {
        "This project has no dbt seeds. Rebind a newer accepted release explicitly with dr_dbt_sources(project, list(orders = accepted_raw), name = 'raw')."
      },
      "stg_orders casts source types; core_orders names order_amount and derives is_positive_order.",
      "customer_revenue aggregates the core model by customer_id.",
      "dr_dbt_sources() manages only its marked source YAML; edit the SQL models normally.",
      "Close R connections to this catalog before running dbt. Reconnect afterwards.",
      "profiles.yml contains machine-specific paths and is intentionally gitignored.",
      "The schema macro uses exact schema names. Use a separate catalog for each environment.",
      "dbt models are mutable and are not automatically dataraft releases."
    ),
    file.path(path, "README.md")
  )
  project
}


dbt_starter_schemas <- function(config, sourced) {
  rlang::local_error_call(rlang::caller_env())
  roles <- c("raw", "staging", "core", "marts")
  schemas <- stats::setNames(roles, roles)
  if (!is.null(names(config$layers))) {
    mapped <- intersect(roles, names(config$layers))
    schemas[mapped] <- unname(config$layers[mapped])
  }
  if (!identical(schemas[["raw"]], "raw")) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "The ingestion schema must be raw.",
      "dr_dbt_invalid"
    )
  }
  if (sourced && !all(schemas %in% config$layers)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "Configure raw, staging, core and marts layers before creating a RAW-source starter.",
      "dr_dbt_invalid"
    )
  }
  if (anyDuplicated(schemas)) {
    dataraft.core::abort(
      subclass = "dataraft_error_dbt",
      "The four dbt starter layers need distinct schema names.",
      "dr_dbt_invalid"
    )
  }
  schemas
}
