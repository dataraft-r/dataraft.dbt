# dataraft.dbt 0.1.0.9005

* The adapter capability protocol no longer declares partition.

# dataraft.dbt 0.1.0.9004

* Implement provider S3 methods for core integration.
* Move static example assets into inst/templates and add a bounded manifest-to-contract draft importer.

* Use the umbrella CI manifest as the single immutable family dependency lock.

# dataraft.dbt 0.1.0.9000

* `dr_dbt_publish()` versions source code, configuration and dependencies without including invocation-dependent compiled SQL. Artifact integrity checks still cover compiled SQL.

* Keep stateless helpers private and prefix shared implementation interfaces with `dr_internal_`. Move component tests into their owning repository; add minimal and downstream CI.

* Move contract-to-dbt schema regression tests into the independently checked component.

* Initial independent DataRaft package.
