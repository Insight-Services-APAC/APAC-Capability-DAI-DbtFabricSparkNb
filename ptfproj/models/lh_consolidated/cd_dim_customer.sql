
-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
select 
a.CustomerID as CustomerKey,
a.CustomerName,
b.CustomerCategoryName,
c.BuyingGroupName,
d.CityName,
e.StateProvinceName,
e.SalesTerritory,
f.CountryName,
f.Continent,
f.Region,
f.SubRegion,
GETDATE() as ETL_Date
from {{ ref('cf_customers') }} a
join {{ ref('cf_customercategories') }} b
 on a.CustomerCategoryID = b.CustomerCategoryID
join {{ ref('cf_buyinggroups') }} c
 on c.BuyingGroupID = a.BuyingGroupID
join {{ ref('cf_cities') }} d
 on d.CityID = a.DeliveryCityID
join {{ ref('cf_stateprovinces') }} e
 on e.StateProvinceID = d.StateProvinceID
join {{ ref('cf_countries') }} f
 on f.CountryID = e.CountryID

