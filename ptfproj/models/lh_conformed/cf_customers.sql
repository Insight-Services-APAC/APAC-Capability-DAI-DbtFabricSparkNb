
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental', incremental_strategy="insert_overwrite",file_format="delta") }}

with source_data as (

select 
CustomerID,
CustomerName,
BillToCustomerID,
CustomerCategoryID,
BuyingGroupID,
PrimaryContactPersonID,
AlternateContactPersonID,
DeliveryMethodID,
DeliveryCityID,
PostalCityID,
CreditLimit,
AccountOpenedDate,
StandardDiscountPercentage,
IsStatementSent,
IsOnCreditHold,
PaymentDays,
PhoneNumber,
FaxNumber,
DeliveryRun,
RunPosition,
WebsiteURL,
DeliveryAddressLine1,
DeliveryAddressLine2,
DeliveryPostalCode,
PostalAddressLine1,
PostalAddressLine2,
PostalPostalCode,
LastEditedBy,
ValidFrom,
ValidTo
from lh_raw.sales_customers

)

select *
from source_data


