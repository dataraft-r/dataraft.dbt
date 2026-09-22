# dataraft.dbt 0.1.0.9000

* `dr_dbt_publish()` versions source code, configuration and dependencies without including invocation-dependent compiled SQL. Artifact integrity checks still cover compiled SQL.

* Keep stateless helpers private and prefix shared implementation interfaces with `dr_internal_`. Move component tests into their owning repository; add minimal and downstream CI.

* Move contract-to-dbt schema regression tests into the independently checked component.

* Initial independent DataRaft package.

* Diagnostic providers now implement public S3 methods; status, quality and lineage no longer require reverse calls from core into extension packages.
