{% test assert_no_negative_amounts(model, column_name) %}

/*
    Generic test: assert_no_negative_amounts
    ----------------------------------------
    Fails if any row has a negative value in the specified column.
    Used on transaction_amount and balance columns to catch data quality issues
    before they propagate into dimensions and facts.

    Usage in .yml:
        tests:
          - assert_no_negative_amounts
*/

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}

