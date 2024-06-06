
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}

with source_data as (

select 
CustomerCategoryID,
CustomerCategoryName,
LastEditedBy,
ValidFrom,
ValidTo
from lh_raw.sales_customercategories

)

select *
from source_data


