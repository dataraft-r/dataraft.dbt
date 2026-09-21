test_that("minimal contracts export dbt schema and executable null and key tests", {
  schema <- dataraft.core::dr_contract(
    columns = c(id = "integer", amount = "numeric"),
    key = "id",
    column_metadata = list(amount = list(description = "Order value"))
  )
  value <- dr_dbt_contract(schema, "orders")
  expect_type(value, "list")
  expect_identical(value$version, 2L)
  model <- value$models[[1]]
  expect_true(model$config$contract$enforced)
  expect_false(model$config$contract$alias_types)
  expect_identical(model$columns[[1]]$data_type, "integer")
  expect_identical(model$columns[[1]]$data_tests, list("not_null", "unique"))
  expect_identical(model$columns[[2]]$data_type, "double")
  expect_identical(model$columns[[2]]$description, "Order value")
  expect_false(model$config$meta$dataraft$r_policies$allow_empty)
  expect_identical(
    dr_dbt_contract(
      schema,
      "orders",
      types = c(amount = "decimal(18,2)")
    )$models[[
      1
    ]]$columns[[2]]$data_type,
    "decimal(18,2)"
  )
})

test_that("quality engines and composite keys are never silently discarded", {
  schema <- dataraft.core::dr_contract(
    columns = c(id = "integer", amount = "numeric"),
    key = c("id", "amount"),
    rules = list(
      dataraft.core::dr_quality_rule("positive", ~ amount > 0),
      dataraft.core::dr_pointblank_checks("external", function(data) data)
    )
  )
  expect_error(
    dr_dbt_contract(schema, "orders"),
    "positive.*external.*composite key",
    class = "dr_dbt_contract_untranslated"
  )
  expect_warning(
    value <- dr_dbt_contract(schema, "orders", unsupported = "report"),
    "positive.*external.*composite key",
    class = "dr_dbt_contract_untranslated"
  )
  expect_length(value$models[[1]]$config$meta$dataraft$untranslated, 3L)
  schema <- dataraft.core::dr_contract(
    columns = c(id = "integer"),
    required = character(),
    key = "id"
  )
  expect_error(dr_dbt_contract(schema, "orders"), "nullable unique key")
})

test_that("SQL type mappings are explicit and drafts cannot enforce schemas", {
  schema <- dataraft.core::dr_contract(
    columns = c(id = "integer64", details = "list")
  )
  expect_error(
    dr_dbt_contract(schema, "nested"),
    "explicit SQL types for: details"
  )
  value <- dr_dbt_contract(schema, "nested", types = c(details = "json"))
  expect_identical(value$models[[1]]$columns[[1]]$data_type, "bigint")
  expect_identical(value$models[[1]]$columns[[2]]$data_type, "json")
  for (overrides in list(
    c("json"),
    stats::setNames("json", ""),
    stats::setNames("json", NA_character_),
    c(details = "json", details = "varchar"),
    c(unknown = "json"),
    c(details = "json; drop table x")
  )) {
    expect_error(
      dr_dbt_contract(schema, "nested", types = overrides),
      "types must name declared columns"
    )
  }
  draft <- dataraft.core::dr_contract_from(data.frame(id = 1L), "draft")
  expect_error(dr_dbt_contract(draft, "draft_model"), "contract_confirm")
  expect_false(
    dr_dbt_contract(draft, "draft_model", enforced = FALSE)$models[[
      1
    ]]$config$contract$enforced
  )
})

test_that("contract exports survive ordinary YAML serialization", {
  skip_if_not_installed("yaml")
  schema <- dataraft.core::dr_contract(
    columns = c(id = "integer", date = "Date", time = "POSIXct")
  )
  path <- withr::local_tempfile(fileext = ".yml")
  yaml::write_yaml(dr_dbt_contract(schema, "events"), path)
  value <- yaml::read_yaml(path)
  expect_identical(
    vapply(value$models[[1]]$columns, `[[`, character(1), "data_type"),
    c("integer", "date", "timestamptz")
  )
})
