{{ config(materialized="table") }}
{{ config(schema="data_staging") }}


select
    cast(order_id as string) as order_id,
    cast(order_item_id as string) as order_item_id,
    cast(product_id as string) as product_id,
    cast(seller_id as string) as seller_id,
    parse_timestamp('%Y-%m-%d %H:%M:%S', shipping_limit_date) as shipping_limit_date,
    cast(price as numeric) as price,
    cast(freight_value as numeric) as freight_value
    
from {{ source("ecommerce_data", "olist_order_items") }}
