with produtos as (
    select 
        oi.order_id,
        oi.order_item_id,
        pr.product_id,
        pr.product_category_name,
        pr.product_category_name_english,
        oi.seller_id,
        oi.shipping_limit_date,
        oi.price,
        oi.freight_value,
        oi.total_value
    from {{ref('stg_orders_itens')}} oi
    left join {{ref('stg_products')}} pr
    on oi.product_id = pr.product_id
), pedidos as (
    select  
        ord.order_id,
        ord.customer_id,
        cus.customer_unique_id,
        cus.customer_city,
        cus.customer_state,
        ord.order_status,
        ord.order_purchase_timestamp,
        ord.order_approved_at,
        ord.order_delivered_carrier_date,
        ord.order_delivered_customer_date,
        ord.order_estimated_delivery_date,
        ord.dias_ate_aprovacao,
        ord.dia_entrega_final,
        ord.dias_estimativa_real
    from {{ref('stg_orders')}} ord
    left join {{ref('stg_customers')}} cus
    on ord.customer_id = cus.customer_id

), fato as (

    select 
        pe.customer_unique_id,
        pe.customer_id,
        pe.order_id,
        pr.order_item_id,
        pr.product_id,
        pr.seller_id,

        pe.order_purchase_timestamp,
        pe.order_approved_at,
        pr.shipping_limit_date,
        pe.order_delivered_carrier_date,
        pe.order_delivered_customer_date,
        pe.order_estimated_delivery_date,
        pe.dias_ate_aprovacao,
        pe.dia_entrega_final,
        pe.dias_estimativa_real,

        pe.customer_city,
        pe.customer_state,
        pe.order_status,
        pr.product_category_name,
        pr.product_category_name_english,

        pr.price,
        pr.freight_value,
        pr.total_value
    from pedidos pe 
    inner join produtos pr 
    on pe.order_id = pr.order_id
)    

select * from fato