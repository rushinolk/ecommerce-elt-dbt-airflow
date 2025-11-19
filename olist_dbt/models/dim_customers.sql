with markup as (
    select *,
    ROW_NUMBER()
    OVER(PARTITION BY customer_unique_id
    ORDER BY customer_id DESC) as rn
    from {{ref('stg_customers')}}  
), final as (
    select * from markup where rn = 1
)

select * from final