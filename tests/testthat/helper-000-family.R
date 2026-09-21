# Test bindings for this package; unavailable optional packages are not loaded.
family_owners <- c(
  "dbt_artifact_hashes" = "dataraft.dbt",
  "dr_execute_transform" = "dataraft.core",
  "dr_add_source" = "dataraft.core",
  "dr_add_transform" = "dataraft.core",
  "dr_inspect" = "dataraft.core",
  "dr_contract_from" = "dataraft.core",
  "dr_contract_confirm" = "dataraft.core",
  "dr_contract" = "dataraft.core",
  "dr_quality_rule" = "dataraft.core",
  "dr_pointblank_checks" = "dataraft.core",
  "dr_dbt_contract" = "dataraft.dbt",
  "dr_dbt_init" = "dataraft.dbt",
  "dbt_write_managed" = "dataraft.dbt",
  "dr_dbt_publish" = "dataraft.dbt",
  "dbt_publication_model" = "dataraft.dbt",
  "dr_dbt_sources" = "dataraft.dbt",
  "dbt_catalog_fingerprint" = "dataraft.dbt",
  "dr_dbt_project" = "dataraft.dbt",
  "dr_dbt_build" = "dataraft.dbt",
  "dr_dbt_test" = "dataraft.dbt",
  "dbt_process" = "dataraft.dbt",
  "dbt_read_artifacts" = "dataraft.dbt",
  "dr_dbt_status" = "dataraft.dbt",
  "dr_dbt_lineage" = "dataraft.dbt",
  "dr_dbt_model" = "dataraft.dbt",
  "dr_quality" = "dataraft.core",
  "dr_releases" = "dataraft.lake",
  "dr_lineage" = "dataraft.core",
  "dr_publish" = "dataraft.core",
  "dr_collect" = "dataraft.core",
  "dr_ingest" = "dataraft.lake",
  "dr_run" = "dataraft.core",
  "dr_product" = "dataraft.core",
  "resolve_release" = "dataraft.lake",
  "dr_tbl" = "dataraft.lake",
  "dr_registry_duckdb" = "dataraft.lake",
  "dr_storage_local" = "dataraft.lake",
  "dr_lake_config" = "dataraft.lake",
  "dr_connect_lake" = "dataraft.lake",
  "dr_disconnect_lake" = "dataraft.lake",
  "dr_close_lake" = "dataraft.lake",
  "dr_read_release" = "dataraft.lake",
  "dr_transform_dbt" = "dataraft.dbt",
  "meta" = "dataraft.lake",
  "dr_execute" = "dataraft.core"
)
for (name in names(family_owners)) {
  owner <- family_owners[[name]]
  if (requireNamespace(owner, quietly = TRUE)) {
    assign(name, get(name, asNamespace(owner), inherits = FALSE))
  }
}
local_family_bindings <- function(..., .package = NULL, .env = parent.frame()) {
  bindings <- list(...)
  if (
    !is.null(.package) && !.package %in% c("dataraft", unique(family_owners))
  ) {
    return(do.call(
      testthat::local_mocked_bindings,
      c(bindings, list(.package = .package, .env = .env))
    ))
  }
  owners <- unname(family_owners[names(bindings)])
  if (anyNA(owners)) {
    stop("Unknown mocked family binding")
  }
  for (owner in unique(owners)) {
    do.call(
      testthat::local_mocked_bindings,
      c(bindings[owners == owner], list(.package = owner, .env = .env))
    )
  }
}
