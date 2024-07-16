
-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="append",file_format="delta") }}
select *
from {{ ref('my_first_dbt_model') }}
where id = 1
