with stg_address as (
    select *
    from {{ ref('address') }}
),

stg_stateprovince as (
    select *
    from {{ ref('stateprovince') }}
),

stg_countryregion as (
    select *
    from {{ ref('countryregion') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['a.addressid']) }} as address_key,
    a.addressid,
    a.city as city_name,
    sp.name as state_name,
    cr.name as country_name
from stg_address a
left join stg_stateprovince sp on a.stateprovinceid = sp.stateprovinceid
left join stg_countryregion cr on sp.countryregioncode = cr.countryregioncode