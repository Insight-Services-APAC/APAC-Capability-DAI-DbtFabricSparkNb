{{ config(materibized='incremental', incrementb_strategy="insert_overwrite",file_format="delta") }}
with source_data as (

select 
a.CustomerID as CustomerKey,
a.BillToCustomerID as BillToCustomerKey,
a.DeliveryMethodID as DeliveryMethodKey,
a.OrderID,
a.InvoiceID,
b.InvoiceLineID,
a.SalespersonPersonID as SalespersonPersonID,
replace(cast(a.InvoiceDate as date),'-', '') as InvoiceDateKey,
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
current_timestamp() as ETL_Date,
row_number () over (
    partition by 
    a.CustomerID,
    a.BillToCustomerID,
    a.DeliveryMethodID,
    a.OrderID,
    a.InvoiceID,
    b.InvoiceLineID,
    a.SalespersonPersonID,
    a.InvoiceDate,
    a.CustomerPurchaseOrderNumber,
    a.IsCreditNote,
    a.CreditNoteReason,
    b.StockItemID,
    b.Quantity,
    b.UnitPrice,
    b.TaxRate,
    b.TaxAmount,
    b.LineProfit,
    b.ExtendedPrice
    order by
    a.ETL_Date desc,
    b.ETL_Date desc
    ) as LatestRecord
from {{ ref('cf_sales_invoicelines') }} b
join {{ ref('cf_sales_invoices') }} a
 on a.InvoiceID = b.InvoiceID

)

select 
    CustomerKey,
    BillToCustomerKey,
    DeliveryMethodKey,
    OrderID,
    InvoiceID,
    InvoiceLineID,
    SalespersonPersonID,
    InvoiceDateKey,
    CustomerPurchaseOrderNumber,
    IsCreditNote,
    CreditNoteReason,
    StockItemKey,
    Quantity,
    UnitPrice,
    TaxRate,
    TaxAmount,
    LineProfit,
    ExtendedPrice,
    SalesAmount,
    ETL_Date
from source_data
where LatestRecord = 1


