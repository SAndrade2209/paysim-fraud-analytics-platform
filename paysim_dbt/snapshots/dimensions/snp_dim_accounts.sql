{% snapshot snp_dim_accounts %}

{{
    config(
        unique_key='account_id',
        strategy='timestamp',
        updated_at='_updated_timestamp',
        invalidate_hard_deletes=True
    )
}}

select
    account_id,
    account_role,
    first_seen_date_key,
    last_seen_date_key,
    is_active,
    _inserted_date,
    _created_timestamp,
    _updated_timestamp
from {{ ref('dim_accounts') }}

{% endsnapshot %}