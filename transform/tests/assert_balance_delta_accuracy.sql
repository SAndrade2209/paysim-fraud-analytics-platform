/*
    Singular test: assert_balance_delta_accuracy
    ---------------------------------------------
    Validates that the derived orig_balance_delta column is arithmetically
    consistent with orig_balance_before and orig_balance_after.
    Allows a ±1 cent rounding tolerance.
*/
select
    transaction_id,
    orig_balance_before,
    orig_balance_after,
    orig_balance_delta
from {{ ref('stg_transactions') }}
where abs(orig_balance_delta - (orig_balance_before - orig_balance_after)) > 0.01
