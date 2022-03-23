with orders as (

    select * from {{ ref('stg_orders') }}

),
payments as (

    select * from {{ ref('stg_payments') }}

),
payments_amount as (

    select 
        order_id,
        sum(amount) as amount
    from payments
    where status = 'success'
    group by order_id
),
final as (

    select
        orders.order_id,
        orders.customer_id,
        coalesce(payments_amount.amount, 0) as amount
    from orders
    left join payments_amount using (order_id) 

)
select * from final
