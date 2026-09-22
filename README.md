# dataraft.dbt

This integration is **experimental**. Pin the DataRaft family and dbt adapter versions together. A dbt build is not an atomic DataRaft release; use managed publication for checked release governance.

Configure and execute dbt projects and inspect their artifacts. Definitions use `dr_dbt_project()`; execution requires dbt and processx. Managed publication integrates with dataraft.lake.

This is an independently installable DataRaft component. The `dataraft`
metapackage provides the shared introduction and re-exports the family API.
See `help(package = "dataraft.dbt")` for the component reference.

Install the development version:

```r
install.packages("pak")
pak::pak("dataraft-r/dataraft.dbt")
```

[Get started with DataRaft](https://github.com/dataraft-r/dataraft).
