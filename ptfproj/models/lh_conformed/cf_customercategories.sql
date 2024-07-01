


{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}

with source_data as (

select 
CustomerCategoryID,
CustomerCategoryName,
LastEditedBy,
ValidFrom,
ValidTo,
current_date() as updated_at
from lh_raw.sales_customercategories

)

select *
from source_data


