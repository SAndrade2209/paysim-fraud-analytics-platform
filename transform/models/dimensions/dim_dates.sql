{{ config(
    materialized='table'
) }}

with dates as (
    select
        dateadd(day, seq4(), to_date('2020-01-01')) as date_day
    from table(generator(rowcount => 3650))

)
select
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    monthname(date_day) as month_name,
    day(date_day) as day_of_month,
    dayofweek(date_day) as day_of_week,
    dayname(date_day) as day_name,
    weekofyear(date_day) as week_of_year,
    iff(dayofweek(date_day) in (0,6), true, false) as is_weekend
from dates
order by date_day