-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
select 
a.PersonID as SalesPersonKey,
a.FullName,
GETDATE() as ETL_Date
from {{ ref('cf_people') }} a