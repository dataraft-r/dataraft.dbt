test_that("dbt status methods preserve node and process outcomes", {
  dbt <- structure(
    list(
      success = FALSE,
      results = tibble::tibble(
        unique_id = letters[1:5],
        status = c("success", "warn", "fail", "error", "skipped")
      )
    ),
    class = "dr_dbt_result"
  )
  expect_equal(
    dataraft.core::dr_status(dbt)$outcome,
    c("succeeded", "succeeded", "blocked", "failed", "skipped", "failed")
  )
  dbt$success <- TRUE
  dbt$results <- dbt$results[0, ]
  expect_equal(nrow(dataraft.core::dr_status(dbt)), 0L)
  expect_type(dataraft.core::dr_status(dbt)$outcome, "character")
})
