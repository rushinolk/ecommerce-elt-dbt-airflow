select 
    *,
    (price + freight_value) as total_value
from {{source('sources','olist_order_items_dataset')}}