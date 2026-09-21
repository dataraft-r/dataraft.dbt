dbt_publication_fixture <- function(marts = FALSE) {
  f <- fixture()
  if (marts) {
    f$lake$config$layers <- c(f$lake$config$layers, "marts")
  }
  DBI::dbExecute(f$lake$con, "CREATE SCHEMA lake.marts")
  DBI::dbExecute(
    f$lake$con,
    paste(
      "CREATE TABLE lake.marts.customer_revenue AS",
      "SELECT 101 AS customer_id, 100.0::DOUBLE AS revenue"
    )
  )
  parsed <- dbt_read_artifacts(system.file(
    "extdata",
    "dbt-artifacts",
    package = "dataraft.dbt"
  ))
  artifacts <- file.path(f$root, "dbt-artifacts")
  dir.create(artifacts)
  file.copy(
    list.files(
      system.file("extdata", "dbt-artifacts", package = "dataraft.dbt"),
      full.names = TRUE
    ),
    artifacts
  )
  f$dbt <- structure(
    list(
      success = TRUE,
      status = 0L,
      command = "build",
      artifacts_dir = artifacts,
      invocation_id = parsed$manifest$metadata$invocation_id,
      artifact_hashes = dbt_artifact_hashes(artifacts),
      results = parsed$results,
      manifest = parsed$manifest
    ),
    class = "dr_dbt_result"
  )
  f
}

# Simulate another completed invocation with matching files and parsed objects.
dbt_publication_new_artifacts <- function(result) {
  path <- result$artifacts_dir
  runs <- jsonlite::read_json(file.path(path, "run_results.json"))
  runs$metadata$invocation_id <- result$manifest$metadata$invocation_id
  jsonlite::write_json(
    result$manifest,
    file.path(path, "manifest.json"),
    auto_unbox = TRUE,
    null = "null"
  )
  jsonlite::write_json(
    runs,
    file.path(path, "run_results.json"),
    auto_unbox = TRUE,
    null = "null"
  )
  parsed <- dbt_read_artifacts(path)
  result$manifest <- parsed$manifest
  result$results <- parsed$results
  result$invocation_id <- parsed$manifest$metadata$invocation_id
  result$artifact_hashes <- dbt_artifact_hashes(path)
  result
}

test_that("minimal dbt publication returns an exact collectable release", {
  f <- dbt_publication_fixture()
  withr::defer(fixture_cleanup(f))
  release <- dr_dbt_publish(f$lake, f$dbt, "customer_revenue")
  expect_identical(release$status, "published")
  expect_identical(release$asset, "customer_revenue")
  expect_true(DBI::dbIsValid(f$lake$con))
  expect_identical(release$outputs$database, "lake")
  expect_identical(release$outputs$schema, "products")
  expect_identical(release$outputs$release_id, release$release_id)
  reference <- resolve_release(f$lake, release$asset, release$release_id)
  expect_identical(release$outputs$table, reference$table_name[[1L]])
  expect_equal(dr_collect(release)$revenue, 100)
  expect_length(release$metadata$contract$required, 0L)
  expect_length(release$metadata$contract$key, 0L)
  expect_null(release$metadata$contract$max_age_hours)
  expect_true(release$metadata$contract$allow_empty)
  DBI::dbExecute(
    f$lake$con,
    "UPDATE lake.marts.customer_revenue SET revenue = 200"
  )
  expect_equal(dr_collect(release)$revenue, 100)
  expect_equal(
    dr_collect(dr_dbt_publish(f$lake, f$dbt, "customer_revenue"))$revenue,
    200
  )
})

test_that("config publication closes owned handles and prefers configured marts", {
  f <- dbt_publication_fixture(marts = TRUE)
  withr::defer(fixture_cleanup(f))
  config <- f$lake$config
  dr_disconnect_lake(f$lake)
  release <- dr_dbt_publish(config, f$dbt, "customer_revenue")
  expect_null(release$output_lake)
  expect_identical(release$outputs$schema, "marts")
  expect_equal(dr_collect(release)$revenue, 100)
  con <- dr_connect_lake(config)
  withr::defer(dr_disconnect_lake(con))
  expect_equal(
    dr_read_release(con, release$asset, release$release_id)$revenue,
    100
  )
})

test_that("automatic versions track definitions but ignore invocation timestamps", {
  f <- dbt_publication_fixture()
  withr::defer(fixture_cleanup(f))
  first <- dr_dbt_publish(f$lake, f$dbt, "customer_revenue")
  f$dbt$manifest$metadata$invocation_id <- "another-successful-invocation"
  f$dbt$manifest$metadata$generated_at <- "2026-10-01T00:00:00Z"
  f$dbt <- dbt_publication_new_artifacts(f$dbt)
  repeated <- dr_dbt_publish(f$lake, f$dbt, "customer_revenue")
  expect_identical(repeated$metadata$code_version, first$metadata$code_version)
  expect_identical(repeated$metadata$version, first$metadata$version)
  f$dbt$manifest$nodes[["model.shop.customer_revenue"]]$compiled_code <-
    "SELECT customer_id, revenue FROM a_changed_input"
  f$dbt <- dbt_publication_new_artifacts(f$dbt)
  changed <- dr_dbt_publish(f$lake, f$dbt, "customer_revenue")
  expect_false(identical(
    changed$metadata$code_version,
    first$metadata$code_version
  ))
  expect_false(identical(changed$metadata$version, first$metadata$version))
  DBI::dbExecute(
    f$lake$con,
    "ALTER TABLE lake.marts.customer_revenue ADD COLUMN currency VARCHAR"
  )
  evolved <- dr_dbt_publish(f$lake, f$dbt, "customer_revenue")
  expect_false(identical(
    evolved$metadata$contract$version,
    changed$metadata$contract$version
  ))
  expect_false(identical(evolved$metadata$version, changed$metadata$version))
  DBI::dbExecute(f$lake$con, "DELETE FROM lake.marts.customer_revenue")
  expect_equal(
    nrow(dr_collect(dr_dbt_publish(f$lake, f$dbt, "customer_revenue"))),
    0L
  )
})

test_that("invalid invocations and final contracts preserve the consumer release", {
  f <- dbt_publication_fixture()
  withr::defer(fixture_cleanup(f))
  first <- dr_dbt_publish(f$lake, f$dbt, "customer_revenue")
  failed <- f$dbt
  failed$success <- FALSE
  expect_error(
    dr_dbt_publish(f$lake, failed, "customer_revenue"),
    "successful dbt build"
  )
  absent <- f$dbt
  absent$results <- absent$results[
    absent$results$unique_id != "model.shop.customer_revenue",
  ]
  expect_error(
    dr_dbt_publish(f$lake, absent, "customer_revenue"),
    "result changed"
  )
  wrong <- f$dbt
  wrong$invocation_id <- "unrelated-invocation"
  expect_error(
    dr_dbt_publish(f$lake, wrong, "customer_revenue"),
    "artifacts changed"
  )
  expect_error(
    dr_dbt_publish(f$lake, f$dbt, "+customer_revenue"),
    "exact dbt unique ID"
  )
  ambiguous <- f$dbt
  ambiguous$manifest$nodes[["model.other.customer_revenue"]] <-
    ambiguous$manifest$nodes[["model.shop.customer_revenue"]]
  expect_error(
    dr_dbt_publish(f$lake, ambiguous, "customer_revenue"),
    "result changed"
  )
  expect_error(
    dbt_publication_model(ambiguous$manifest, "customer_revenue"),
    "unambiguous"
  )
  DBI::dbExecute(
    f$lake$con,
    "UPDATE lake.marts.customer_revenue SET revenue = -10"
  )
  checked <- dr_contract(
    columns = c(customer_id = "integer", revenue = "numeric"),
    rules = list(dr_quality_rule("positive", ~ revenue > 0))
  )
  blocked <- dr_dbt_publish(
    f$lake,
    f$dbt,
    "customer_revenue",
    contract = checked,
    stop_on_failure = FALSE
  )
  expect_identical(blocked$status, "blocked")
  expect_identical(blocked$metadata$contract$id, "customer_revenue.contract")
  expect_null(blocked$outputs)
  expect_error(dr_collect(blocked), "no successful output")
  expect_equal(dr_collect(first)$revenue, 100)
  expect_identical(
    resolve_release(f$lake, first$asset)$release_id[[1L]],
    first$release_id
  )
  expect_true(DBI::dbIsValid(f$lake$con))
})
