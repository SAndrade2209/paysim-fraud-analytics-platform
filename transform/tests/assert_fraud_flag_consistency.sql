/*
    Singular test: assert_fraud_flag_consistency
    ---------------------------------------------
    If a transaction is flagged as fraud (is_flagged_fraud = true) it MUST
    also be marked as actual fraud (is_fraud = true) in the staging layer.
    Any row where is_flagged_fraud=true but is_fraud=false indicates a
    data quality issue upstream — return those rows to fail the test.
*/

select
    transaction_id,
    is_fraud,
    is_flagged_fraud
from {{ ref('stg_transactions') }}
where is_flagged_fraud = true
  and is_fraud = false

