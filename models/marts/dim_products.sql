with 

stg_product as (
    select * from {{ ref('product') }}
),

stg_product_category as (
    select * from {{ ref('productcategory') }}
),

stg_product_subcategory as (
    select * from {{ ref('productsubcategory') }}
)


select 
    {{ dbt_utils.generate_surrogate_key(['p.productid']) }} as product_key,
    p.productid,
    p.name as product_name,
    p.productnumber,
    p.color,
    p.class,
    psc.name as product_subcategory_name,
    pc.name as product_category_name
from stg_product p
left join stg_product_subcategory psc on psc.productsubcategoryid = p.productsubcategoryid
left join stg_product_category pc on pc.productcategoryid = psc.productcategoryid