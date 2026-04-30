{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge',
        merge_update_columns=[
            'account_origin',
            'account_destination',
            'time_step',
            'transaction_type',
            'transaction_amount',
            'orig_balance_before',
            'orig_balance_after',
            'orig_balance_delta',
            'dest_balance_before',
            'dest_balance_after',
            'dest_balance_delta',
            'is_fraud',
            'is_flagged_fraud',
            '_kitchen_ingestion_timestamp'
        ]
    )
}}

with source as (
    select * from {{ source('raw', 'raw_transactions') }}

    {% if is_incremental() %}
    -- Solo trae registros cuyo timestamp de ingesta RAW supera el último procesado en kitchen
    where _raw_ingestion_timestamp >= (
        select dateadd(minute, -5, coalesce(max(_kitchen_ingestion_timestamp), '1900-01-01'))
        from {{ this }}
    )
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
                    'step',
                    'type',
                    'amount',
                    'nameorig',
                    'namedest'
                ]) }} as transaction_id,

        -- identifiers
        nameorig                                        as account_origin,
        namedest                                        as account_destination,

        -- transaction attributes
        step                                            as time_step,
        type                                            as transaction_type,
        amount                                          as transaction_amount,

        -- balances origin
        oldbalanceorg                                  as orig_balance_before,
        newbalanceorig                                  as orig_balance_after,
        round(oldbalanceorg - newbalanceorig, 2)       as orig_balance_delta,

        -- balances destination
        oldbalancedest                                  as dest_balance_before,
        newbalancedest                                  as dest_balance_after,
        round(newbalancedest - oldbalancedest, 2)       as dest_balance_delta,

        -- fraud flags
        isfraud::boolean                                as is_fraud,
        isflaggedfraud::boolean                         as is_flagged_fraud,

        -- metadata
        current_date() as _ingestion_date,
        _raw_ingestion_timestamp,
        current_timestamp()                             as _kitchen_ingestion_timestamp

    from source
),
deduped as (
    select * from renamed
        qualify row_number() over (partition by transaction_id order by _raw_ingestion_timestamp desc
    ) = 1
)
select * from deduped