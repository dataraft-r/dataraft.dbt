test_that("dbt releases preserve snapshots and revalidate mutable source relations", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  DBI::dbExecute(f$lake$con, "CREATE SCHEMA lake.marts")
  DBI::dbExecute(
    f$lake$con,
    "CREATE TABLE lake.marts.customer_revenue AS SELECT 101 AS customer_id, 100.0::DOUBLE AS revenue"
  )
  parsed <- dataraft.dbt:::dbt_read_artifacts(system.file(
    "extdata",
    "dbt-artifacts",
    package = "dataraft.dbt"
  ))
  result <- structure(
    list(
      success = TRUE,
      status = 0L,
      command = "build",
      artifacts_dir = system.file(
        "extdata",
        "dbt-artifacts",
        package = "dataraft.dbt"
      ),
      invocation_id = parsed$manifest$metadata$invocation_id,
      artifact_hashes = dbt_artifact_hashes(system.file(
        "extdata",
        "dbt-artifacts",
        package = "dataraft.dbt"
      )),
      results = parsed$results,
      manifest = parsed$manifest
    ),
    class = "dr_dbt_result"
  )
  contract <- dr_contract(
    "revenue",
    "1",
    "Analytics",
    "Customer revenue",
    "One customer",
    c(customer_id = "integer", revenue = "numeric"),
    key = "customer_id",
    rules = list(dr_quality_rule("positive", function(data) {
      counts <- dplyr::collect(dplyr::summarise(data, n = sum(revenue < 0)))
      counts$n == 0
    }))
  )
  first <- dr_dbt_publish(
    f$lake,
    result,
    "model.shop.customer_revenue",
    contract,
    "shop.revenue",
    code_version = "v1"
  )
  expect_equal(first$status, "published")
  DBI::dbExecute(
    f$lake$con,
    "UPDATE lake.marts.customer_revenue SET revenue = -10"
  )
  second <- dr_dbt_publish(
    f$lake,
    result,
    "model.shop.customer_revenue",
    contract,
    "shop.revenue",
    code_version = "v1",
    stop_on_failure = FALSE
  )
  expect_equal(second$status, "blocked")
  expect_equal(dplyr::collect(dr_tbl(f$lake, "shop.revenue"))$revenue, 100)
  expect_equal(dr_releases(f$lake, "shop.revenue")$release_id, first$release_id)
  expect_setequal(dr_quality(first)$stage, c("model", "candidate"))
  edges <- dr_lineage(f$lake, "shop.revenue")
  expect_equal(edges$from_id, "model.shop.customer_revenue")
  expect_equal(edges$from_version, parsed$manifest$metadata$invocation_id)
  result$command <- "test"
  expect_snapshot(
    error = TRUE,
    dr_dbt_publish(
      f$lake,
      result,
      "model.shop.customer_revenue",
      contract,
      "shop.revenue",
      code_version = "v1"
    )
  )
})
