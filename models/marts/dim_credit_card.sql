with stg_salesorderheader as (
    select distinct creditcardid
    from {{ ref('salesorderheader') }}
    where creditcardid is not null
),

stg_creditcard as (
    select *
    from {{ ref('creditcard') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['soh.creditcardid']) }} as creditcard_key,
    soh.creditcardid,
    cc.cardtype
from stg_salesorderheader soh
left join stg_creditcard cc on soh.creditcardid = cc.creditcardid