select
    agent_id,
    agent_name,
    agent_email,
    broker_id
from {{ source('silver', 'agent') }}