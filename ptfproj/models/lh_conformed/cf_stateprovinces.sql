
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}

with source_data as (

select 
StateProvinceID,
StateProvinceCode,
StateProvinceName,
CountryID,
SalesTerritory,
LatestRecordedPopulation,
LastEditedBy,
ValidFrom,
ValidTo,
current_timestamp() as ETL_Date
from lh_raw.application_stateprovinces

)

select *
from source_data


