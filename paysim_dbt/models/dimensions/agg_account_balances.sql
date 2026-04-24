{{
    config(
        materialized='incremental',
        unique_key='account_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        merge_update_columns=[
            'current_balance',
            'total_debited',
            'total_credited',
            'net_flow',
            'debit_transaction_count',
            'credit_transaction_count',
            'total_transaction_count',
            'last_movement_date_key',
            '_updated_timestamp'
        ]
    )
}}

/*
    agg_account_balances
    --------------------
    Tracks the current and cumulative balance state for every account
    derived exclusively from valid (non-fraud) movements in fct_balance_movements.

    Balance logic:
      - ORIGIN role  → account sends money  → balance_after = orig_balance_after  (debit)
      - DESTINATION role → account receives money → balance_after = dest_balance_after (credit)

    Current balance = the latest observed balance_after across both roles,
    using time_step as the tiebreaker.
*/

with movements as (

    select * from {{ ref('fct_balance_movements') }}

    {% if is_incremental() %}
    where _kitchen_ingestion_timestamp >= (
        select coalesce(max(_updated_timestamp), '1900-01-01')
        from {{ this }}
    )
    {% endif %}

),

-- Each row where the account acts as the sender (debit)
debits as (

    select
        origin_account_id                  as account_id,
        date_key,
        transaction_amount                 as amount,
        origin_balance_after               as balance_after,
        row_number() over (
            partition by origin_account_id
            order by time_step desc, _kitchen_ingestion_timestamp desc
        ) = 1                              as is_latest_movement
    from movements
    where origin_account_id is not null

),

-- Each row where the account acts as the receiver (credit)
credits as (

    select
        destination_account_id             as account_id,
        date_key,
        transaction_amount                 as amount,
        destination_balance_after          as balance_after,
        row_number() over (
            partition by destination_account_id
            order by time_step desc, _kitchen_ingestion_timestamp desc
        ) = 1                              as is_latest_movement
    from movements
    where destination_account_id is not null

),

-- Aggregate debits per account
debit_summary as (

    select
        account_id,
        sum(amount)                        as total_debited,
        count(*)                           as debit_transaction_count,
        max(date_key)                      as last_debit_date_key,
        max(case when is_latest_movement then balance_after end) as latest_debit_balance
    from debits
    group by account_id

),

-- Aggregate credits per account
credit_summary as (

    select
        account_id,
        sum(amount)                        as total_credited,
        count(*)                           as credit_transaction_count,
        max(date_key)                      as last_credit_date_key,
        max(case when is_latest_movement then balance_after end) as latest_credit_balance
    from credits
    group by account_id

),

-- Union all accounts seen in either role
all_accounts as (

    select account_id from debit_summary
    union
    select account_id from credit_summary

),

joined as (

    select
        a.account_id,

        -- Current balance: pick the balance from the most recent role
        -- If last seen as origin, use latest_debit_balance; as destination, use latest_credit_balance
        -- When both exist, the more recent date_key wins
        case
            when d.last_debit_date_key is null
                then c.latest_credit_balance
            when c.last_credit_date_key is null
                then d.latest_debit_balance
            when d.last_debit_date_key >= c.last_credit_date_key
                then d.latest_debit_balance
            else
                c.latest_credit_balance
        end                                                     as current_balance,

        coalesce(d.total_debited, 0)                            as total_debited,
        coalesce(c.total_credited, 0)                           as total_credited,
        coalesce(c.total_credited, 0)
            - coalesce(d.total_debited, 0)                      as net_flow,

        coalesce(d.debit_transaction_count, 0)                  as debit_transaction_count,
        coalesce(c.credit_transaction_count, 0)                 as credit_transaction_count,
        coalesce(d.debit_transaction_count, 0)
            + coalesce(c.credit_transaction_count, 0)           as total_transaction_count,

        greatest(
            coalesce(d.last_debit_date_key, 0),
            coalesce(c.last_credit_date_key, 0)
        )                                                       as last_movement_date_key,

        current_timestamp()                                     as _updated_timestamp

    from all_accounts a
    left join debit_summary  d on a.account_id = d.account_id
    left join credit_summary c on a.account_id = c.account_id

)

select *
from joined

