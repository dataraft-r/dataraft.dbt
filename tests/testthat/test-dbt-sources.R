dbt_source_config <- function(root) {
  dr_lake_config(
    dr_registry_duckdb(file.path(root, "lake.db")),
    dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = "duckdb",
    layers = c("raw", "staging", "core", "marts")
  )
}

test_that("explicit dbt sources pin registry relations and preserve other names", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("yaml")
  root <- withr::local_tempdir()
  writeLines("name: example", file.path(root, "dbt_project.yml"))
  project <- dr_dbt_project(root)
  config <- dbt_source_config(root)
  orders <- dr_ingest(data.frame(id = 1L), to = config, name = "orders")
  customers <- dr_ingest(data.frame(id = 2L), to = config, name = "customers")
  expect_identical(
    dr_dbt_sources(project, list(orders = orders, customers = customers)),
    project
  )
  path <- file.path(root, "models", "dataraft_sources_inputs.yml")
  source <- yaml::read_yaml(path)$sources[[1]]
  expect_identical(source$database, "lake")
  expect_identical(source$schema, "raw")
  expect_identical(source$tables[[1]]$identifier, orders$outputs$table)
  expect_identical(
    source$tables[[1]]$config$meta$dataraft$release_id,
    orders$release_id
  )
  expect_true(all(unlist(source$quoting)))
  newer <- dr_ingest(data.frame(id = 3L), to = config, name = "orders")
  newer$outputs$table <- "edited_description_is_not_authority"
  dr_dbt_sources(project, list(orders = newer))
  tables <- yaml::read_yaml(path)$sources[[1]]$tables
  by_name <- stats::setNames(tables, vapply(tables, `[[`, character(1), "name"))
  expect_false(identical(by_name$orders$identifier, newer$outputs$table))
  expect_identical(
    by_name$orders$config$meta$dataraft$release_id,
    newer$release_id
  )
  expect_identical(by_name$customers$identifier, customers$outputs$table)
  expect_length(list.files(root, recursive = TRUE, pattern = "\\.sql$"), 0L)
})

test_that("invalid source identity preserves the previous source file", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  writeLines("name: example", file.path(root, "dbt_project.yml"))
  project <- dr_dbt_project(root)
  config <- dbt_source_config(root)
  accepted <- dr_ingest(data.frame(id = 1L), to = config, name = "orders")
  other <- dr_ingest(data.frame(id = 2L), to = config, name = "other")
  dr_dbt_sources(project, list(orders = accepted))
  path <- file.path(root, "models", "dataraft_sources_inputs.yml")
  previous <- readLines(path)
  rejected <- accepted
  rejected$status <- "blocked"
  expect_error(
    dr_dbt_sources(project, list(orders = accepted, bad = rejected)),
    "successful immutable"
  )
  mismatched <- accepted
  mismatched$release_id <- other$release_id
  expect_error(
    dr_dbt_sources(project, list(orders = accepted, bad = mismatched)),
    "exact release"
  )
  # Even replacing asset and release together cannot impersonate another run.
  mismatched$asset <- other$asset
  expect_error(
    dr_dbt_sources(project, list(orders = accepted, bad = mismatched)),
    "exact release"
  )
  expect_identical(readLines(path), previous)
  expect_error(dr_dbt_sources(project, list(accepted)), "named, non-empty")
  expect_error(
    dr_dbt_sources(project, list(orders = accepted, orders = accepted)),
    "named, non-empty"
  )
})

test_that("dbt sources protect handwritten YAML and catalog identity", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  writeLines("name: example", file.path(root, "dbt_project.yml"))
  dir.create(file.path(root, "models"))
  path <- file.path(root, "models", "dataraft_sources_inputs.yml")
  writeLines("# My source definitions", path)
  config <- dbt_source_config(root)
  project <- dr_dbt_project(root)
  accepted <- dr_ingest(data.frame(id = 1L), to = config, name = "orders")
  expect_error(
    dr_dbt_sources(project, list(orders = accepted)),
    "not package-owned"
  )
  expect_identical(readLines(path), "# My source definitions")
  other <- accepted
  other$output_config$catalog$path <- file.path(root, "other.db")
  expect_error(
    dr_dbt_sources(project, list(orders = accepted, other = other)),
    "same catalog"
  )
  project$source_config <- config
  expect_error(
    dr_dbt_sources(project, list(orders = other)),
    "different project catalog"
  )
  ducklake <- dr_lake_config(backend = "ducklake")
  alternative <- ducklake
  alternative$storage$path <- file.path(root, "other-data")
  expect_false(identical(
    dbt_catalog_fingerprint(ducklake),
    dbt_catalog_fingerprint(alternative)
  ))
})
