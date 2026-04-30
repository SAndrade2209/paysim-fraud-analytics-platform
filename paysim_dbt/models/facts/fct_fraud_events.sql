{{
    config(
        materialized='incremental',
        unique_key='fraud_event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with source as (

    select *
    from {{ ref('stg_transactions') }}

    where is_fraud = true
       or is_flagged_fraud = true

    {% if is_incremental() %}
    and _kitchen_ingestion_timestamp >= (
        select coalesce(max(_created_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

joined as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'account_origin',
            'account_destination',
            'transaction_type',
            'time_step',
            'transaction_amount'
        ]) }} as fraud_event_id,

        dd.date_key,

        ao.account_id as origin_account_id,
        ad.account_id as destination_account_id,

        tt.transaction_type_code,

        source.transaction_id,
        source.time_step,
        source.transaction_amount,

        source.orig_balance_before,
        source.orig_balance_after,

        source.dest_balance_before,
        source.dest_balance_after,

        source.is_fraud,
        source.is_flagged_fraud,

        case
            when source.is_fraud = true
             and source.is_flagged_fraud = true
                then 'true_positive'

            when source.is_fraud = true
             and source.is_flagged_fraud = false
                then 'false_negative'

            when source.is_fraud = false
             and source.is_flagged_fraud = true
                then 'false_positive'

            else 'other'
        end as fraud_event_type,

        current_timestamp() as _created_timestamp

    from source

    left join {{ ref('dim_dates') }} dd
        on source._ingestion_date = dd.date_day

    left join {{ ref('dim_accounts') }} ao
        on source.account_origin = ao.account_id

    left join {{ ref('dim_accounts') }} ad
        on source.account_destination = ad.account_id

    left join {{ ref('dim_transaction_types') }} tt
        on upper(source.transaction_type) = tt.transaction_type_code

)

select *
from joined