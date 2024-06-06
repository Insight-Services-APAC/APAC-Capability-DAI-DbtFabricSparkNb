
-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
select 
a.CustomerID,
a.CustomerName,
a.BillToCustomerID,
a.CustomerCategoryID,
b.CustomerCategoryName
from {{ ref('cf_customers') }} a
join {{ ref('cf_customercategories') }} b
 on a.CustomerCategoryID = b.CustomerCategoryID

