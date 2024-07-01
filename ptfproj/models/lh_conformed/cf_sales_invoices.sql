

{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}

with source_data as (

select 
InvoiceID,
CustomerID,
BillToCustomerID,
OrderID,
DeliveryMethodID,
ContactPersonID,
AccountsPersonID,
SalespersonPersonID,
PackedByPersonID,
InvoiceDate,
CustomerPurchaseOrderNumber,
IsCreditNote,
CreditNoteReason,
Comments,
DeliveryInstructions,
InternalComments,
TotalDryItems,
TotalChillerItems,
DeliveryRun,
RunPosition,
ReturnedDeliveryData,
ConfirmedDeliveryTime,
ConfirmedReceivedBy,
LastEditedBy,
LastEditedWhen,
current_timestamp() as ETL_Date
from lh_raw.sales_invoices

)

select *
from source_data


