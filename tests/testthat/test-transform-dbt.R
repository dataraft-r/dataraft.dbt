test_that("dbt transform definitions do not connect or execute", {
  calls <- 0
  step <- dr_transform_dbt(
    dr_dbt_project("missing"),
    "model.shop.orders",
    connection = function() {
      calls <<- calls + 1
    },
    input = DBI::Id(schema = "raw", table = "orders")
  )
  expect_equal(dr_inspect(step)$type, "dbt transformation")
  expect_equal(dr_inspect(step)$input, list(schema = "raw", table = "orders"))
  expect_equal(calls, 0)
  error <- tryCatch(
    dr_transform_dbt(
      dr_dbt_project("missing"),
      "orders",
      connection = identity,
      input = "orders"
    ),
    error = identity
  )
  expect_match(conditionMessage(error), "exact dbt unique ID", fixed = TRUE)
})

test_that("real dbt transformation consumes staged input and propagates test failures", {
  executable <- Sys.getenv("DATARAFT_DBT_EXECUTABLE")
  skip_if(
    !nzchar(executable),
    "Set DATARAFT_DBT_EXECUTABLE for the dbt CLI integration test"
  )
  skip_if_not_installed("duckdb")
  skip_if_not_installed("yaml")
  root <- withr::local_tempdir()
  database <- file.path(root, "analytics.duckdb")
  dir.create(file.path(root, "models"))
  yaml::write_yaml(
    list(
      name = "shop",
      version = "1.0.0",
      `config-version` = 2L,
      profile = "shop",
      `model-paths` = list("models"),
      models = list(shop = list(`+materialized` = "table"))
    ),
    file.path(root, "dbt_project.yml")
  )
  yaml::write_yaml(
    list(
      shop = list(
        target = "dev",
        outputs = list(
          dev = list(
            type = "duckdb",
            path = database,
            schema = "main",
            threads = 1L
          )
        )
      )
    ),
    file.path(root, "profiles.yml")
  )
  writeLines(
    "select id, amount * 2 as amount from staged_orders",
    file.path(root, "models", "orders.sql")
  )
  yaml::write_yaml(
    list(
      version = 2L,
      models = list(list(
        name = "orders",
        columns = list(list(name = "id", tests = list("unique", "not_null")))
      ))
    ),
    file.path(root, "models", "schema.yml")
  )
  connections <- list()
  factory <- function() {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = database)
    connections[[length(connections) + 1L]] <<- con
    con
  }
  step <- dr_transform_dbt(
    dr_dbt_project(root, root, executable = executable),
    "model.shop.orders",
    connection = factory,
    input = "staged_orders"
  )
  first_run <- dr_product("dbt.orders") |>
    dr_add_source(data.frame(id = 1:2, amount = c(10, 20))) |>
    dr_add_transform(step, "dbt") |>
    dr_add_transform(identity, "after_dbt") |>
    dr_run()
  first <- dr_collect(first_run)
  expect_equal(
    first_run$metadata$transformations$dbt$model,
    "model.shop.orders"
  )
  expect_equal(first$amount, c(20, 40))
  expect_null(attr(first, "dr_transform_metadata"))
  expect_null(first_run$metadata$transformations$after_dbt)
  second <- dr_execute_transform(step, data.frame(id = 1:2, amount = c(15, 25)))
  expect_equal(second$amount, c(30, 50))
  expect_equal(any(vapply(connections, DBI::dbIsValid, logical(1))), FALSE)
  error <- tryCatch(
    dr_execute_transform(step, data.frame(id = c(1L, 1L), amount = c(10, 20))),
    error = identity
  )
  expect_s3_class(error, "dr_dbt_failed")
  expect_equal(error$result$success, FALSE)
})
