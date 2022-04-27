select
    cancelled_at,
    count(1)
from {{ ref('stg_subscriptions' )}}
where status='CANCELLED' and cancelled_at is not null
group by 1 having count(1)>1
