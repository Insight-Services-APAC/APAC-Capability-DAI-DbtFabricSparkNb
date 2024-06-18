
-- Use the `ref` function to select from other models
{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}
with source_data as (

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
row_number () over (
    partition by a.CustomerID,
    a.CustomerName,
    b.CustomerCategoryName,
    c.BuyingGroupName,
    d.CityName,
    e.StateProvinceName,
    e.SalesTerritory,
    f.CountryName,
    f.Continent,
    f.Region,
    f.SubRegion
    order by
    a.ETL_Date desc,
    b.ETL_Date desc,
    c.ETL_Date desc,
    d.ETL_Date desc,
    e.ETL_Date desc,
    f.ETL_Date desc
    ) as LatestRecord
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

)

select 
    CustomerKey,
    CustomerName,
    CustomerCategoryName,
    BuyingGroupName,
    CityName,
    StateProvinceName,
    SalesTerritory,
    CountryName,
    Continent,
    Region,
    SubRegion
from source_data
where LatestRecord = 1

