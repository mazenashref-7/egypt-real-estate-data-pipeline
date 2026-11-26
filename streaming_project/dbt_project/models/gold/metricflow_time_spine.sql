{{ config(materialized='table') }}

select date_day::date as date_day
from generate_series(
    timestamp '2000-01-01',
    timestamp '2035-12-31',
    interval '1 day'
) as date_day
