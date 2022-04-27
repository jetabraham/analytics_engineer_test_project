with source as
         (
             select
                    id,
                    date(created_at)   as created_dt,
                    created_at,
                    customer_id,
                    customer_id ||COALESCE(sku,'NA') as subscription_id,
                    sku,
                    email,
                    product_title,
                    price,
                    status,
                    quantity,
                    updated_at,
                    cancelled_at,
                    date(cancelled_at) as cancelled_dt,
                    cancellation_reason
             from {{ source('SourceMedium', 'subscriptions') }}
        ),

final as (
    select source.*,
    sum(case when status ='ACTIVE' then 1 else 0 end) over(partition by created_dt) as subscriptions_active,
    lead(status)over(partition by customer_id order by created_dt desc) as previous_status,
    case when status ='ACTIVE' then 1 else 0 end as active_subscriptions,
    max(status)over(partition by  customer_id,product_title) as max_status_product,
    max(status)over(partition by  created_dt,customer_id,product_title) as max_status,
    from source
)
select * from final
