test_that("dbt results and lineage are available without dbt", {
  path <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
  status <- dr_dbt_status(path)
  expect_equal(nrow(status), 3L)
  expect_equal(status$status, rep("success", 3))
  expect_equal(
    dr_dbt_lineage(path)$from,
    c("seed.shop.raw_orders", "model.shop.stg_orders")
  )
  expect_equal(
    dr_dbt_lineage(path)$to,
    c("model.shop.stg_orders", "model.shop.customer_revenue")
  )
})

test_that("each invocation gets isolated artifacts and literal selector arguments", {
  root <- withr::local_tempdir()
  writeLines("name: test", file.path(root, "dbt_project.yml"))
  paths <- character()
  captured <- NULL
  local_family_bindings(dbt_process = function(
    command,
    args,
    wd,
    echo,
    timeout
  ) {
    captured <<- args
    target <- args[match("--target-path", args) + 1L]
    paths <<- c(paths, target)
    fixtures <- system.file(
      "extdata",
      "dbt-artifacts",
      package = "dataraft.dbt"
    )
    file.copy(list.files(fixtures, full.names = TRUE), target)
    list(status = 0L, stdout = "done", stderr = "")
  })
  project <- dr_dbt_project(root, executable = file.path(R.home("bin"), "R"))
  result <- dr_execute(
    project,
    select = "tag:monthly orders; echo nope",
    echo = FALSE
  )
  expect_equal(result$success, TRUE)
  expect_equal(
    captured[match("--select", captured) + 1L],
    "tag:monthly orders; echo nope"
  )
  expect_equal(dr_dbt_status(result)$status, rep("success", 3))
  second <- dr_dbt_test(project, echo = FALSE)
  expect_equal(second$command, "test")
  expect_length(unique(paths), 2L)
})

test_that("a failed process cannot reuse previous successful artifacts", {
  root <- withr::local_tempdir()
  writeLines("name: test", file.path(root, "dbt_project.yml"))
  dir.create(file.path(root, "target"))
  fixtures <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
  file.copy(list.files(fixtures, full.names = TRUE), file.path(root, "target"))
  local_family_bindings(dbt_process = function(...) {
    list(status = 2L, stdout = "", stderr = "bad profile")
  })
  result <- dr_dbt_build(
    dr_dbt_project(root, executable = file.path(R.home("bin"), "R")),
    echo = FALSE,
    stop_on_failure = FALSE
  )
  expect_equal(result$success, FALSE)
  expect_equal(result$status, 2L)
  expect_equal(nrow(result$results), 0L)
  expect_match(result$artifact_error, "Missing dbt artifact")
})

test_that("inconsistent artifacts are rejected", {
  root <- withr::local_tempdir()
  fixtures <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
  file.copy(list.files(fixtures, full.names = TRUE), root)
  runs <- jsonlite::read_json(file.path(root, "run_results.json"))
  runs$metadata$invocation_id <- "another-run"
  jsonlite::write_json(
    runs,
    file.path(root, "run_results.json"),
    auto_unbox = TRUE,
    null = "null"
  )
  expect_snapshot(error = TRUE, dr_dbt_status(root))
})

test_that("selectors cannot inject CLI flags", {
  expect_snapshot(
    error = TRUE,
    dr_dbt_build(dr_dbt_project("."), select = "--profiles-dir")
  )
})

test_that("dbt failures retain structured diagnostics", {
  root <- withr::local_tempdir()
  writeLines("name: test", file.path(root, "dbt_project.yml"))
  local_family_bindings(dbt_process = function(command, args, ...) {
    target <- args[match("--target-path", args) + 1L]
    fixtures <- system.file(
      "extdata",
      "dbt-artifacts",
      package = "dataraft.dbt"
    )
    file.copy(list.files(fixtures, full.names = TRUE), target)
    runs <- jsonlite::read_json(file.path(target, "run_results.json"))
    runs$results[[1]]$status <- "fail"
    jsonlite::write_json(
      runs,
      file.path(target, "run_results.json"),
      auto_unbox = TRUE,
      null = "null"
    )
    list(status = 0L, stdout = "", stderr = "")
  })
  project <- dr_dbt_project(root, executable = file.path(R.home("bin"), "R"))
  error <- tryCatch(
    dr_dbt_build(project, echo = FALSE),
    dr_dbt_failed = identity
  )
  expect_s3_class(error, "dr_dbt_failed")
  expect_equal(error$result$results$status[[1]], "fail")
  expect_equal(error$result$success, FALSE)
})

test_that("dbt relations become a lazy dm with explicit keys", {
  skip_if_not_installed("dm")
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  DBI::dbExecute(f$lake$con, "CREATE SCHEMA lake.marts")
  DBI::dbExecute(
    f$lake$con,
    "CREATE TABLE lake.marts.customer_revenue AS SELECT 101 AS customer_id, 100 AS revenue"
  )
  path <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
  model <- dr_dbt_model(
    f$lake,
    path,
    tables = c(revenue = "model.shop.customer_revenue"),
    primary_keys = list(revenue = "customer_id")
  )
  expect_s3_class(model, "dm")
  expect_equal(dplyr::collect(model$revenue)$revenue, 100L)
  expect_equal(
    attr(model, "dr_dbt_nodes"),
    c(revenue = "model.shop.customer_revenue")
  )
})


test_that("malformed optional fields cannot erase failed nodes", {
  root <- withr::local_tempdir()
  fixtures <- system.file("extdata", "dbt-artifacts", package = "dataraft.dbt")
  file.copy(list.files(fixtures, full.names = TRUE), root)
  runs <- jsonlite::read_json(file.path(root, "run_results.json"))
  runs$results[[1]]$status <- "fail"
  runs$results[[1]]$failures <- list()
  jsonlite::write_json(
    runs,
    file.path(root, "run_results.json"),
    auto_unbox = TRUE,
    null = "null"
  )
  expect_snapshot(error = TRUE, dr_dbt_status(root))
})
