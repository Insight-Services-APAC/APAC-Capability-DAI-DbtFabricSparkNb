
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}

with source_data as (

select 
InvoiceLineID,
InvoiceID,
StockItemID,
Description,
PackageTypeID,
Quantity,
UnitPrice,
TaxRate,
TaxAmount,
LineProfit,
ExtendedPrice,
LastEditedBy,
LastEditedWhen
from lh_raw.sales_invoicelines

)

select *
from source_data

