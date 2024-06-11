
-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
select 
a.DeliveryMethodID as DeliveryMethodKey,
a.DeliveryMethodName,
GETDATE() as ETL_Date
from {{ ref('cf_deliverymethods') }} a

