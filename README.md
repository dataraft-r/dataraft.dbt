# dataraft.dbt

**Bring an existing dbt project into a DataRaft workflow.**

Use this experimental integration when your SQL models already live in dbt. It can describe a dbt project, run builds and inspect artifacts. Managed publication requires a configured DataRaft lake. Running dbt alone does not create an atomic checked DataRaft release.

[`dataraft` overview](https://github.com/dataraft-r/dataraft) · [dbt reference](https://dataraft-r.github.io/dataraft/components/dataraft.dbt/reference/index.html)

## Describe a project

```r
library(dataraft.dbt)

project_dir <- tempfile("dbt-project-")
dir.create(project_dir)
writeLines("name: example", file.path(project_dir, "dbt_project.yml"))
project <- dr_dbt_project(project_dir)
```

This constructs a project definition. To run `dr_dbt_build(project)`, provide a real dbt project, a dbt executable and the optional execution dependency `processx`. Use managed publication when outputs need DataRaft release checks and evidence.

Install the development package with `pak::pak("dataraft-r/dataraft.dbt")`. See the [integration guide](https://dataraft-r.github.io/dataraft/articles/integrations.html) and [lake package](https://github.com/dataraft-r/dataraft.lake).
