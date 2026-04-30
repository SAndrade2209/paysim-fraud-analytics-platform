{{ config(
    materialized='table'
) }}

with source_codes as (

    select distinct upper(trim(transaction_type)) as transaction_type_code
    from {{ ref('stg_transactions') }}
    where transaction_type is not null

),

prepared as (

    select

        transaction_type_code,

        case transaction_type_code
            when 'CASH_IN' then 'Cash deposited into account'
            when 'CASH_OUT' then 'Cash withdrawn from account'
            when 'TRANSFER' then 'Transfer between accounts'
            when 'PAYMENT' then 'Merchant payment'
            when 'DEBIT' then 'Direct debit'
            else 'Unknown transaction type'
        end as transaction_type_description,

        case
            when transaction_type_code in ('CASH_IN','CASH_OUT')
                then true
            else false
        end as is_cash_related,

        true as is_active

    from source_codes

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'transaction_type_code',
        ]) }} as transaction_type_id,

        transaction_type_code,
        transaction_type_description,
        is_cash_related,
        is_active,

        current_date()                              as _inserted_date,
        current_timestamp()      as _created_timestamp,
        current_timestamp()         as _updated_timestamp

    from prepared

)

select *
from final