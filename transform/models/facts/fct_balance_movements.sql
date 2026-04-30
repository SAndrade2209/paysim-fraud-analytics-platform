{{
    config(
        materialized='incremental',
        unique_key='movement_id',
        incremental_strategy='append',
        on_schema_change='append_new_columns'
    )
}}

with source as (

    select *
    from {{ ref('stg_transactions') }}
    where is_fraud = false
          and is_flagged_fraud = false

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
        ]) }} as movement_id,

        dd.date_key,

        ao.account_id as origin_account_id,
        ad.account_id as destination_account_id,

        source.transaction_id,
        source.transaction_type,
        source.time_step,

        source.transaction_amount,

        source.orig_balance_before     as origin_balance_before,
        source.orig_balance_after      as origin_balance_after,
        source.orig_balance_delta      as origin_balance_change,

        source.dest_balance_before     as destination_balance_before,
        source.dest_balance_after      as destination_balance_after,
        source.dest_balance_delta      as destination_balance_change,

        current_timestamp()                    as _created_timestamp,
        _kitchen_ingestion_timestamp           as _kitchen_ingestion_timestamp

    from source

    left join {{ ref('dim_dates') }} dd
        on source._ingestion_date = dd.date_day

    left join {{ ref('dim_accounts') }} ao
        on source.account_origin = ao.account_id

    left join {{ ref('dim_accounts') }} ad
        on source.account_destination = ad.account_id


)

select *
from joined