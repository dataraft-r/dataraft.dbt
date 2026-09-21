managed_dbt_fixture <- function(root) {
  config <- dr_lake_config(
    dr_registry_duckdb(file.path(root, "lake.db")),
    dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = "duckdb",
    layers = c("raw", "staging", "core", "marts")
  )
  path <- file.path(root, "analytics")
  dir.create(path)
  writeLines(
    c("name: shop", "profile: insurance", "model-paths: [sql]"),
    file.path(path, "dbt_project.yml")
  )
  dir.create(file.path(path, "sql"))
  writeLines("select 1 as untouched", file.path(path, "sql", "manual.sql"))
  writeLines("# My private profile", file.path(path, "profiles.yml"))
  list(config = config, path = path)
}

managed_dbt_process <- function(command, args, ...) {
  target <- args[match("--target-path", args) + 1L]
  fixtures <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
  file.copy(list.files(fixtures, full.names = TRUE), target)
  list(status = 0L, stdout = "done", stderr = "")
}

test_that("managed project creation, updates and inspection are deferred", {
  root <- withr::local_tempdir()
  config <- dr_lake_config(
    dr_registry_duckdb(file.path(root, "missing.db")),
    dr_storage_local(file.path(root, "data")),
    backend = "duckdb"
  )
  ref <- structure(
    list(
      asset = "orders",
      release_id = "release",
      run_id = "run",
      status = "published",
      output_config = config,
      data = data.frame(secret = "not retained")
    ),
    class = "dr_run_result"
  )
  local_family_bindings(dr_connect_lake = function(...) {
    stop("Unexpected connection")
  })
  project <- dr_dbt_project(
    file.path(root, "not-created"),
    lake = config,
    sources = list(orders = ref)
  )
  expect_false(dir.exists(project$path))
  expect_false(file.exists(config$catalog$path))
  expect_identical(names(project$source_groups), "inputs")
  expect_null(project$source_groups$inputs$orders$data)
  updated <- dr_dbt_sources(project, list(customers = ref), name = "inputs")
  expect_named(updated$source_groups$inputs, c("orders", "customers"))
  expect_named(project$source_groups$inputs, "orders")
  expect_identical(dr_inspect(updated)$status, "defined")
  expect_output(print(project), "managed duckdb")
  expect_error(
    dr_dbt_project(root, lake = config, profiles_dir = root),
    "Choose lake"
  )
})

test_that("managed execution resolves any layer, owns files and closes its handle", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  f <- managed_dbt_fixture(root)
  accepted <- dr_ingest(data.frame(id = 1L), to = f$config, name = "orders")
  enriched <- dr_publish(
    data.frame(id = 1L),
    "enriched",
    to = f$config,
    layer = "staging"
  )
  # Qualified descriptions are not an authority for source selection.
  enriched$outputs <- list(
    database = "elsewhere",
    schema = "fake",
    table = "fake"
  )
  project <- dr_dbt_project(
    f$path,
    lake = f$config,
    sources = list(enriched = enriched),
    executable = file.path(R.home("bin"), "R")
  ) |>
    dr_dbt_sources(list(orders = accepted), name = "raw")
  before <- readLines(file.path(f$path, "dbt_project.yml"))
  captured <- NULL
  local_family_bindings(dbt_process = function(command, args, ...) {
    # A writable connection can open only after the registry reader is closed.
    connection <- dr_connect_lake(f$config)
    dr_close_lake(connection)
    captured <<- args
    managed_dbt_process(command, args, ...)
  })
  result <- dr_run(project, echo = FALSE)
  expect_true(result$success)
  expect_identical(result$project, project)
  expect_identical(result$source_bindings$inputs$enriched$schema, "staging")
  expect_identical(
    result$source_bindings$inputs$enriched$release_id,
    enriched$release_id
  )
  yaml_path <- file.path(f$path, "sql", "dataraft_managed_sources.yml")
  bindings <- yaml::read_yaml(yaml_path)$sources
  expect_identical(
    vapply(bindings, `[[`, character(1), "name"),
    c("inputs", "raw")
  )
  expect_identical(
    vapply(bindings, `[[`, character(1), "schema"),
    c("staging", "raw")
  )
  profiles_dir <- captured[match("--profiles-dir", captured) + 1L]
  expect_true(startsWith(profiles_dir, result$artifacts_dir))
  output <- yaml::read_yaml(file.path(
    profiles_dir,
    "profiles.yml"
  ))$insurance$outputs$dataraft
  expect_identical(output$attach[[1]]$path, f$config$catalog$path)
  expect_identical(output$database, "lake")
  expect_identical(
    readLines(file.path(f$path, "profiles.yml")),
    "# My private profile"
  )
  expect_identical(readLines(file.path(f$path, "dbt_project.yml")), before)
  expect_identical(
    readLines(file.path(f$path, "sql", "manual.sql")),
    "select 1 as untouched"
  )
  # Reconstructed definitions remove undeclared groups instead of inheriting them.
  smaller <- dr_dbt_project(
    f$path,
    lake = f$config,
    sources = list(enriched = enriched),
    executable = file.path(R.home("bin"), "R")
  )
  dr_run(smaller, echo = FALSE)
  expect_length(yaml::read_yaml(yaml_path)$sources, 1L)
})

test_that("managed preflight validates all groups before touching files", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  f <- managed_dbt_fixture(root)
  accepted <- dr_ingest(data.frame(id = 1L), to = f$config, name = "orders")
  enriched <- dr_publish(
    data.frame(id = 1L),
    "enriched",
    to = f$config,
    layer = "staging"
  )
  project <- dr_dbt_project(
    f$path,
    lake = f$config,
    sources = list(raw = accepted, enriched = enriched),
    executable = file.path(R.home("bin"), "R")
  )
  local_family_bindings(dbt_process = function(...) {
    stop("must not execute")
  })
  expect_error(dr_run(project), "one physical schema")
  expect_false(dir.exists(file.path(f$path, ".dataraft")))
  path <- file.path(f$path, "sql", "dataraft_managed_sources.yml")
  expect_false(file.exists(path))
  project <- dr_dbt_project(
    f$path,
    lake = f$config,
    sources = list(orders = accepted),
    executable = file.path(R.home("bin"), "R")
  )
  writeLines("# Handwritten sources", path)
  expect_error(dr_run(project), "not package-owned")
  expect_identical(readLines(path), "# Handwritten sources")
  expect_false(dir.exists(file.path(f$path, ".dataraft")))
  missing <- accepted
  missing$release_id <- "not-in-registry"
  expect_error(
    dr_run(dr_dbt_sources(project, list(orders = missing), name = "inputs")),
    "exact release"
  )
  expect_identical(readLines(path), "# Handwritten sources")
})

test_that("managed source groups reject another catalog without IO", {
  root <- withr::local_tempdir()
  f <- managed_dbt_fixture(root)
  config <- f$config
  config$catalog$path <- file.path(root, "other.db")
  fake <- structure(
    list(
      asset = "orders",
      release_id = "release",
      run_id = "run",
      status = "published",
      output_config = config
    ),
    class = "dr_run_result"
  )
  expect_error(
    dr_dbt_project(f$path, lake = f$config, sources = list(orders = fake)),
    "different project catalog"
  )
  expect_false(file.exists(config$catalog$path))
})

test_that("managed publication infers the lake and verifies artifact provenance", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  f <- managed_dbt_fixture(root)
  accepted <- dr_ingest(data.frame(id = 1L), to = f$config, name = "orders")
  project <- dr_dbt_project(
    f$path,
    lake = f$config,
    sources = list(orders = accepted),
    executable = file.path(R.home("bin"), "R")
  )
  local_family_bindings(dbt_process = function(command, args, ...) {
    lake <- dr_connect_lake(f$config)
    on.exit(dr_close_lake(lake))
    DBI::dbExecute(
      lake$con,
      paste(
        "CREATE OR REPLACE TABLE lake.marts.customer_revenue AS",
        "SELECT 101 AS customer_id, 100.0::DOUBLE AS revenue"
      )
    )
    managed_dbt_process(command, args, ...)
  })
  built <- dr_run(project, echo = FALSE)
  release <- dr_publish(built, "customer_revenue", asset = "shop.revenue")
  expect_identical(release$status, "published")
  expect_identical(release$outputs$schema, "marts")
  expect_identical(release$asset, "shop.revenue")
  expect_equal(dr_collect(release)$revenue, 100)
  other <- f$config
  other$catalog$path <- file.path(root, "another.db")
  expect_error(
    dr_publish(built, "customer_revenue", to = other),
    "same lake catalog"
  )
  expect_false(file.exists(other$catalog$path))
  modified <- built
  modified$manifest$nodes[["model.shop.customer_revenue"]]$alias <- "different"
  expect_error(dr_publish(modified, "customer_revenue"), "result changed")
  failed <- built
  failed$success <- FALSE
  expect_error(dr_publish(failed, "customer_revenue"), "successful dbt build")
  writeLines("{}", file.path(built$artifacts_dir, "manifest.json"))
  expect_error(
    dr_publish(built, "customer_revenue"),
    class = "dr_dbt_artifact_invalid"
  )
  expect_equal(dr_collect(release)$revenue, 100)
})

test_that("source-free managed projects initialize their lake before dbt", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  f <- managed_dbt_fixture(root)
  config <- dr_lake_config(path = file.path(root, "fresh"), backend = "duckdb")
  project <- dr_dbt_project(
    f$path,
    lake = config,
    executable = file.path(R.home("bin"), "R")
  )
  expect_false(dir.exists(file.path(root, "fresh")))
  local_family_bindings(dbt_process = function(command, args, ...) {
    expect_true(file.exists(file.path(root, "fresh", "dataraft.json")))
    lake <- dr_connect_lake(config)
    on.exit(dr_close_lake(lake))
    expect_true(DBI::dbExistsTable(
      lake$con,
      DBI::Id(catalog = "lake", schema = "_dl", table = "runs")
    ))
    managed_dbt_process(command, args, ...)
  })
  expect_true(dr_run(project, echo = FALSE)$success)
})

test_that("failed managed source replacement retains the previous file", {
  skip_if_not_installed("yaml")
  root <- withr::local_tempdir()
  destination <- file.path(root, "sources.yml")
  writeLines("previous bindings", destination)
  prepared <- list(
    profiles = list(example = list(target = "dev")),
    destination = destination,
    source_yaml = "new bindings"
  )
  # A destination directory cannot be replaced by an ordinary file. The writer
  # must leave its prior contents intact and report the failure.
  blocked <- file.path(root, "blocked")
  dir.create(blocked)
  writeLines("retained", file.path(blocked, "previous"))
  prepared$destination <- blocked
  expect_error(
    dbt_write_managed(prepared, file.path(root, "artifacts")),
    "previous file was preserved"
  )
  expect_identical(readLines(file.path(blocked, "previous")), "retained")
  expect_identical(readLines(destination), "previous bindings")
})
