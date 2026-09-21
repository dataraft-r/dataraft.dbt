test_that("starter profiles attach the configured catalog", {
  skip_if_not_installed("yaml")
  root <- withr::local_tempdir()
  for (backend in c("duckdb", "ducklake")) {
    config <- dr_lake_config(
      dr_registry_duckdb(file.path(root, "lake.db")),
      dr_storage_local(file.path(root, "data")),
      backend = backend
    )
    project <- dr_dbt_init(file.path(root, backend), config)
    profile <- yaml::read_yaml(file.path(
      project$path,
      "profiles.yml"
    ))$dataraft_demo$outputs$dev
    expect_equal(profile$database, "lake")
    expect_equal(profile$attach[[1]]$alias, "lake")
    expect_equal(
      profile$attach[[1]]$path,
      paste0(
        if (backend == "ducklake") "ducklake:" else "",
        config$catalog$path
      )
    )
    expect_equal(file.exists(config$catalog$path), FALSE)
  }
})

test_that("starter refuses to overwrite existing files", {
  skip_if_not_installed("yaml")
  root <- withr::local_tempdir()
  writeLines("keep", file.path(root, "important.txt"))
  expect_snapshot(
    error = TRUE,
    dr_dbt_init(root, dr_lake_config(backend = "duckdb"))
  )
  expect_equal(readLines(file.path(root, "important.txt")), "keep")
})

test_that("real dbt builds and tests the starter project", {
  executable <- Sys.getenv("DATARAFT_DBT_EXECUTABLE")
  skip_if(
    !nzchar(executable),
    "Set DATARAFT_DBT_EXECUTABLE for the external CLI integration test"
  )
  root <- withr::local_tempdir()
  backend <- Sys.getenv("DATARAFT_TEST_BACKEND", "duckdb")
  config <- dr_lake_config(
    dr_registry_duckdb(file.path(root, "lake.db")),
    dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = backend
  )
  lake <- dr_connect_lake(config)
  dr_disconnect_lake(lake)
  project <- dr_dbt_init(
    file.path(root, "dbt"),
    config,
    executable = executable
  )
  result <- dr_dbt_build(project, echo = FALSE, stop_on_failure = FALSE)
  expect_equal(
    result$success,
    TRUE,
    info = paste(result$stdout, result$stderr, result$artifact_error)
  )
  if (!result$success) {
    return(invisible(NULL))
  }
  expect_equal(sum(result$results$status == "pass"), 7L)
  expect_equal(dr_dbt_test(project, echo = FALSE)$success, TRUE)
  lake <- dr_connect_lake(config)
  withr::defer(dr_disconnect_lake(lake))
  model <- dr_dbt_model(
    lake,
    result,
    tables = c(revenue = "model.dataraft_demo.customer_revenue"),
    primary_keys = list(revenue = "customer_id")
  )
  expect_equal(sum(dplyr::collect(model$revenue)$revenue), 150)
  contract <- dr_contract_from(
    model$revenue,
    "revenue",
    "Analytics",
    "Customer revenue",
    "One customer",
    key = "customer_id"
  ) |>
    dr_contract_confirm()
  release <- dr_dbt_publish(
    lake,
    result,
    "model.dataraft_demo.customer_revenue",
    contract,
    "shop.revenue",
    code_version = "v1"
  )
  expect_equal(release$status, "published")
  expect_equal(
    sum(
      dplyr::collect(dr_tbl(lake, "shop.revenue", release$release_id))$revenue
    ),
    150
  )
})

test_that("RAW starter binds ingestion releases without creating dbt seeds", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("yaml")
  root <- withr::local_tempdir()
  config <- dr_lake_config(
    dr_registry_duckdb(file.path(root, "lake.db")),
    dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = "duckdb",
    layers = c("raw", "staging", "core", "marts")
  )
  accepted <- dr_ingest(
    data.frame(order_id = 1:2, customer_id = c(1L, 1L), amount = c("10", "20")),
    to = config,
    name = "orders"
  )
  expect_true(all(startsWith(
    accepted$inputs$landed_path,
    paste0(normalizePath(root, winslash = "/"), "/")
  )))
  project <- dr_dbt_init(
    file.path(root, "dbt"),
    config,
    sources = list(orders = accepted)
  )
  expect_false(dir.exists(file.path(project$path, "seeds")))
  staging <- paste(
    readLines(file.path(project$path, "models/staging/stg_orders.sql")),
    collapse = "\n"
  )
  expect_match(staging, "source('raw', 'orders')", fixed = TRUE)
  expect_match(staging, 'cast("amount" as double)', fixed = TRUE)
  expect_match(
    paste(
      readLines(file.path(project$path, "models/core/core_orders.sql")),
      collapse = "\n"
    ),
    "amount as order_amount",
    fixed = TRUE
  )
  expect_match(
    paste(
      readLines(file.path(project$path, "models/marts/customer_revenue.sql")),
      collapse = "\n"
    ),
    "ref('core_orders')",
    fixed = TRUE
  )
  expect_identical(
    yaml::read_yaml(file.path(
      project$path,
      "models/dataraft_sources_raw.yml"
    ))$sources[[1]]$tables[[1]]$identifier,
    accepted$outputs$table
  )
  expect_true(file.exists(config$catalog$path))
  bad <- accepted
  bad$metadata$schema <- c(id = "integer")
  expect_error(
    dr_dbt_init(file.path(root, "bad"), config, sources = list(orders = bad)),
    "order starter requires"
  )
  expect_false(dir.exists(file.path(root, "bad")))
  expect_error(
    dr_dbt_init(
      file.path(root, "generic"),
      config,
      sources = list(customers = accepted)
    ),
    "order starter needs"
  )
  templated <- accepted
  templated$metadata$schema <- c(
    templated$metadata$schema,
    stats::setNames("character", "{{ unsafe }}")
  )
  expect_error(
    dr_dbt_init(
      file.path(root, "templated"),
      config,
      sources = list(orders = templated)
    ),
    "template delimiters"
  )
  expect_false(dir.exists(file.path(root, "templated")))
})

test_that("named lake layers configure the three transformation schemas", {
  skip_if_not_installed("yaml")
  root <- withr::local_tempdir()
  config <- dr_lake_config(
    backend = "duckdb",
    layers = c(
      raw = "raw",
      staging = "prep",
      core = "business",
      marts = "reporting"
    )
  )
  project <- dr_dbt_init(file.path(root, "dbt"), config)
  properties <- yaml::read_yaml(file.path(
    project$path,
    "dbt_project.yml"
  ))$models$dataraft_demo
  expect_identical(properties$staging$`+schema`, "prep")
  expect_identical(properties$core$`+schema`, "business")
  expect_identical(properties$marts$`+schema`, "reporting")
})
