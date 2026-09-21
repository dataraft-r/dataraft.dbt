# dbt releases preserve snapshots and revalidate mutable source relations

    Code
      dr_dbt_publish(f$lake, result, "model.shop.customer_revenue", contract,
      "shop.revenue", code_version = "v1")
    Condition
      Error in `dr_dbt_publish()`:
      ! Publication requires a successful dbt build result.

