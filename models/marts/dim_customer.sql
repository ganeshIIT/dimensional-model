with stg_customer as (
    select
        customerid,
        personid,
        storeid
    from {{ ref('customer') }}
),

stg_person as (
    select
        businessentityid,
        concat(coalesce(firstname, ''), ' ', coalesce(middlename, ''), ' ', coalesce(lastname, '')) as fullname
    from {{ ref('person') }}
),

stg_store as (
    select
        businessentityid as storebusinessentityid,
        storename
    from {{ ref('store') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['c.customerid']) }} as customer_key,
    c.customerid,
    p.businessentityid,
    p.fullname,
    s.storebusinessentityid,
    s.storename
from stg_customer c
left join stg_person p on c.personid = p.businessentityid
left join stg_store s on c.storeid = s.storebusinessentityid