
-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
with source_data as (

select 
a.DateKey,
a.Date,
a.Day,
a.MonthName,
a.MonthNumber,
a.Year,
row_number () over (
    partition by 
    a.DateKey
    order by
    a.ETL_Date desc
    ) as LatestRecord
from {{ ref('cf_dates') }} a

)

select 
    DateKey,
    Date,
    Day,
    MonthName,
    MonthNumber,
    Year
from source_data
where LatestRecord = 1