select
    broker_id,
    broker_name,
    broker_email,
    broker_phone
from {{ source('silver', 'broker') }}