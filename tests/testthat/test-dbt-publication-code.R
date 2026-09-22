test_that("dbt versions describe source definitions, not rendered invocation state", {
  manifest <- list(
    metadata = list(dbt_version = "1.10.0", adapter_type = "duckdb"),
    nodes = list(
      "model.shop.orders" = list(
        unique_id = "model.shop.orders",
        resource_type = "model",
        raw_code = "select '{{ run_started_at }}' as built_at from {{ ref('raw') }}",
        compiled_code = "select '2026-01-01' as built_at from raw",
        config = list(materialized = "table"),
        depends_on = list(nodes = list("model.shop.raw"))
      )
    )
  )
  original <- dbt_publication_code(manifest)
  manifest$metadata$invocation_id <- "next-invocation"
  manifest$nodes[[
    1
  ]]$compiled_code <- "select '2026-01-02' as built_at from raw"
  expect_identical(dbt_publication_code(manifest), original)
  manifest$nodes[[1]] <- rev(manifest$nodes[[1]])
  expect_identical(dbt_publication_code(manifest), original)
  manifest$nodes[[1]]$raw_code <- "select 2 as amount"
  expect_equal(identical(dbt_publication_code(manifest), original), FALSE)
  manifest$nodes[[
    1
  ]]$raw_code <- "select '{{ run_started_at }}' as built_at from {{ ref('raw') }}"
  manifest$nodes[[1]]$config$materialized <- "view"
  expect_equal(identical(dbt_publication_code(manifest), original), FALSE)
})
