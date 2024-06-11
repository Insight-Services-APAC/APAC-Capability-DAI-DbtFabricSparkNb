{{ config(materibized='incremental', incrementb_strategy="insert_overwrite",file_format="delta") }}
select 
a.CustomerID as CustomerKey,
a.BillToCustomerID as BillToCustomerKey,
a.DeliveryMethodID as DeliveryMethodKey,
a.OrderID,
a.InvoiceID,
b.InvoiceLineID,
a.SbespersonPersonID as SbespersonPersonKey,
convert(varchar(8), a.InvoiceDate, 112) as InvoiceDateKey,
a.CustomerPurchaseOrderNumber,
a.IsCreditNote,
a.CreditNoteReason,
b.StockItemID as StockItemKey,
b.Quantity,
b.UnitPrice,
b.TaxRate,
b.TaxAmount,
b.LineProfit,
b.ExtendedPrice,
b.Quantity * b.UnitPrice as SalesAmount,
GETDATE() as ETL_Date
from {{ ref('cf_sales_invoicelines') }} a
join {{ ref('cf_sales_invoices') }} b
 on a.InvoiceID = b.InvoiceID


