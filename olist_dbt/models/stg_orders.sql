with cast_data as (
    select
        order_id,
        customer_id,
        order_status,
        CAST(order_purchase_timestamp as TIMESTAMP) as order_purchase_timestamp,
        CAST(order_approved_at as TIMESTAMP) as order_approved_at ,
        CAST(order_delivered_carrier_date as TIMESTAMP) as order_delivered_carrier_date,
        CAST(order_delivered_customer_date as TIMESTAMP) as order_delivered_customer_date, 
        CAST(order_estimated_delivery_date as TIMESTAMP) as order_estimated_delivery_date
    
    from {{source('sources','olist_orders_dataset')}}
), calculando as (
    select 
        *,
        (order_approved_at - order_purchase_timestamp) as dias_ate_aprovacao,
        (order_delivered_customer_date - order_delivered_carrier_date ) as dia_entrega_final
    from cast_data
), final as (
  select
    *,
    (order_estimated_delivery_date - order_delivered_customer_date) as dias_estimativa_real
  from calculando
)
select * from final