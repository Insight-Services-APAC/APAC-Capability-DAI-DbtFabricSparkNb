


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
LastEditedWhen,
current_timestamp() as ETL_Date
from lh_raw.sales_invoicelines

)

select *
from source_data

