select
    property_id,
    category,
    property_type,
    offering_type
from {{ source('silver', 'property') }}