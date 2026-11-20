select
    li.listing_id,
    li.location_id,
    li.property_id,
    li.agent_id,
    ag.broker_id,
    li.year_month,
    li.listed_date,
    li.bedrooms,
    li.bathrooms,
    li.area_value,
    li.area_unit,
    li.price_egp,
    li.price_period,
    case
        when p.offering_type in ('Residential For Sale', 'Rs') then 'sale'
        when p.offering_type in ('Residential For Rent', 'Rr') then 'rent'
    end as offering_type,
    li.price_egp / fx.usd_egp as price_usd,
    case
        when p.offering_type in ('Residential For Sale', 'Rs')
         and li.area_value > 0
        then li.price_egp / li.area_value
    end as sale_price_per_sqm_egp,
    case
        when p.offering_type in ('Residential For Rent', 'Rr')
         and li.price_period = 'monthly'
         and li.area_value > 0
        then li.price_egp / li.area_value
    end as monthly_rent_per_sqm_egp,
    fx.usd_egp,
    inf.urban_annual_pct
from {{ source('silver', 'listing') }} li
left join {{ source('silver', 'property') }} p on li.property_id = p.property_id
left join {{ source('silver', 'agent') }} ag on li.agent_id = ag.agent_id
left join {{ source('silver', 'usd_egp_monthly') }} fx on li.year_month = fx.year_month
left join {{ source('silver', 'egypt_inflation_monthly') }} inf on li.year_month = inf.year_month