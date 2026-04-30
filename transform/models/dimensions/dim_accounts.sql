{{
    config(
        materialized='incremental',
        unique_key='account_id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge',
        merge_update_columns=[
            'account_role',
            'last_seen_date_key',
            'is_active',
            '_updated_timestamp'
        ]
    )
}}

with stg_events as (
    select
        account_origin     as account_id,
        _raw_ingestion_timestamp,
        _kitchen_ingestion_timestamp
    from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where _kitchen_ingestion_timestamp > (
        select coalesce(max(_updated_timestamp), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

    union all

    select
        account_destination as account_id,
        _raw_ingestion_timestamp,
        _kitchen_ingestion_timestamp
    from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where _kitchen_ingestion_timestamp > (
        select coalesce(max(_updated_timestamp), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}
),

incoming_accounts as (
    select
        account_id,
        min(to_number(to_char(_raw_ingestion_timestamp::date, 'YYYYMMDD'))) as incoming_first_seen_date_key,
        max(to_number(to_char(_raw_ingestion_timestamp::date, 'YYYYMMDD'))) as incoming_last_seen_date_key
    from stg_events
    where account_id is not null
    group by account_id
),

{% if is_incremental() %}
existing_accounts as (
    select account_id, first_seen_date_key, last_seen_date_key
    from {{ this }}
),
{% endif %}

resolved as (
    select
        i.account_id,
        case
            when left(i.account_id, 1) = 'C' then 'customer'
            when left(i.account_id, 1) = 'M' then 'merchant'
            else 'unknown'
        end as account_role,

        -- first_seen: congelado al valor existente, solo se usa incoming si es nuevo
        {% if is_incremental() %}
        coalesce(e.first_seen_date_key, i.incoming_first_seen_date_key) as first_seen_date_key,
        greatest(coalesce(e.last_seen_date_key, i.incoming_last_seen_date_key), i.incoming_last_seen_date_key) as last_seen_date_key,
        {% else %}
        i.incoming_first_seen_date_key as first_seen_date_key,
        i.incoming_last_seen_date_key  as last_seen_date_key,
        {% endif %}

        current_date()      as _inserted_date,
        current_timestamp() as _created_timestamp,
        current_timestamp() as _updated_timestamp

    from incoming_accounts i
    {% if is_incremental() %}
    left join existing_accounts e on i.account_id = e.account_id
    {% endif %}
)

select
    account_id,
    account_role,
    first_seen_date_key,
    last_seen_date_key,
    case
        when last_seen_date_key >= to_number(to_char(current_date - 365, 'YYYYMMDD'))
            then true
        else false
    end as is_active,
    _inserted_date,
    _created_timestamp,
    _updated_timestamp
from resolved
