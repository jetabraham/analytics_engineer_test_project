
with subscriber_latest_status as (
    select
          customer_id,
           STRING_AGG(DISTINCT max_status_product) as agg_status
    from {{ref('stg_subscriptions')}}  group by 1
),
subscriptions_new_return as (
    select
        created_dt,
        sum(case when id is not null then 1 end) as subscriptions_new,
        sum(case when status='ACTIVE' and previous_status='CANCELLED' and sku is not null  then 1 else 0 end) as subscriptions_returning,
        sum(case when previous_status is null then 1 else 0 end) as subscribers_new
    from {{ref('stg_subscriptions')}}
    group by 1
),
cancel as (
    select cancelled_dt,
           customer_id,
           id,
           status
    from {{ref('stg_subscriptions')}}
    where cancelled_dt is not null
),
subscription_cancel as (
    # Subscription Cancels
    select cancelled_dt,
           count(id) as cancel,
    from cancel
    group by 1
),
sub_cancel as (
    # Subscriber Cancel Information and No Active Subscription
    select
       cancelled_dt,
       count(distinct cancel.customer_id) as subscriber_cancel
    from cancel
    left join subscriber_latest_status s on cancel.customer_id = s.customer_id
    where agg_status='CANCELLED'
    group by 1
),
subscription_roll_over_cancel as (
    # Churn based on Cancellation
     select
        cancelled_dt,
        sum(cancel)over(order by cancelled_dt asc ) as subscription_churned
     from subscription_cancel
),
rolling_sub_churned as (
    select
        cancelled_dt,
        sum(subscriber_cancel)over(order by cancelled_dt) as subscribers_churned
    from sub_cancel
),
rolling_subscriber_active as (
    select distinct
        created_dt,
        count(customer_id) over(order by created_dt ) as subscriber_active
    from  {{ref('stg_subscriptions')}}
    where status = 'ACTIVE'
    and max_status <> 'CANCELLED'
    group by 1,customer_id

),
subscriptions_rolling_active as (
     select
         created_dt,
         sum(active_subscriptions)over(order by created_dt ) as sub_active
    from {{ref('stg_subscriptions')}}
),
final as (
    select
        s.created_dt as date,
        max(s.subscriptions_new) subscriptions_new,
        max(s.subscriptions_returning) subscriptions_returning,
        max(cancel) as subscriptions_cancelled,
        max(ra.sub_active) as subscriptions_active,
        max(rc.subscription_churned) as subscription_churned,
        max(s.subscribers_new) subscribers_new,
        max(subscriber_cancel) as subscribers_cancel,
        max(rsa.subscriber_active) as subscribers_active,
        max(sc.subscribers_churned) as subscribers_churned
    from subscriptions_new_return s
    left join subscription_cancel c on s.created_dt = c.cancelled_dt
    left join sub_cancel on s.created_dt = sub_cancel.cancelled_dt
    left join rolling_subscriber_active rsa  on s.created_dt = rsa.created_dt
    left join subscriptions_rolling_active ra on s.created_dt = ra.created_dt
    left join subscription_roll_over_cancel rc on s.created_dt = rc.cancelled_dt
    left join rolling_sub_churned sc on s.created_dt = sc.cancelled_dt
    group by 1
)
select
*
from final
order by 1 desc
