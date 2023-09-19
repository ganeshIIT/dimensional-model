with

stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        cast(orderdate as date) as orderdate 
    from {{ ref('salesorderheader') }}
),

stg_salesorderdetail as (
    select 
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from {{ ref('salesorderdetail') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['sod.salesorderid','sod.salesorderdetailid']) }} as sales_key,
    {{ dbt_utils.generate_surrogate_key(['productid']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} as creditcard_key,
    {{ dbt_utils.generate_surrogate_key(['shiptoaddressid']) }} as ship_address_key,
    {{ dbt_utils.generate_surrogate_key(['order_status']) }} as order_status_key,
    {{ dbt_utils.generate_surrogate_key(['orderdate']) }} as order_date_key,
    sod.salesorderid,
    sod.salesorderdetailid,
    sod.unitprice,
    sod.orderqty,
    sod.revenue
from stg_salesorderdetail sod
join stg_salesorderheader so on so.salesorderid = sod.salesorderid