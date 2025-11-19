select 
    pd.product_id,
    pd.product_category_name,
    tr.product_category_name_english
from {{source('sources','olist_products_dataset')}} pd
left join {{source('sources','product_category_name_translation')}} tr
on pd.Product_category_name = tr.Product_category_name