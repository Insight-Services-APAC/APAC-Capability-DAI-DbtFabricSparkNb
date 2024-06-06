{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
select 
a.InvoiceLineID,
a.InvoiceID,
a.StockItemID,
a.Description,
a.PackageTypeID,
a.Quantity,
a.UnitPrice,
a.TaxRate,
a.TaxAmount,
a.LineProfit,
b.CustomerID,
b.InvoiceDate,
b.DeliveryMethodID
from {{ ref('cf_sales_invoicelines') }} a
join {{ ref('cf_sales_invoices') }} b
 on a.InvoiceID = b.InvoiceID


