select
    location_id,
    city,
    town,
    district,
    subdistrict,
    lat,
    lon
from {{ source('silver', 'location') }}