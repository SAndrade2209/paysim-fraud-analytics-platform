{% snapshot snp_dim_transaction_types %}

{{
    config(
        unique_key='transaction_type_id',
        strategy='check',
        check_cols=[
            'transaction_type_description',
            'is_cash_related',
            'is_active'
        ],
        invalidate_hard_deletes=True
    )
}}

select
    transaction_type_id,
    transaction_type_description,
    is_cash_related,
    is_active
from {{ ref('dim_transaction_types') }}

{% endsnapshot %}