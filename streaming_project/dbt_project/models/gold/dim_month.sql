select
    year_month,
    to_date(year_month || '-01', 'YYYY-MM-DD')                             as month_start,
    extract(year    from to_date(year_month || '-01', 'YYYY-MM-DD'))::int  as year,
    extract(month   from to_date(year_month || '-01', 'YYYY-MM-DD'))::int  as month,
    extract(quarter from to_date(year_month || '-01', 'YYYY-MM-DD'))::int  as quarter
from {{ source('silver', 'egypt_inflation_monthly') }}