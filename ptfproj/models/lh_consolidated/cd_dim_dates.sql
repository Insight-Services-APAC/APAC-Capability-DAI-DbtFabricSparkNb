
-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
select 
a.DateKey,
a.Date,
a.Day,
a.MonthName,
a.MonthNumber,
a.Year,
GETDATE() as ETL_Date
from {{ ref('cf_dates') }} a

