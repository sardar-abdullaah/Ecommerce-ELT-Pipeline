{{ config(materialized="table") }}
{{ config(schema="data_staging") }}

select
    cast(product_category_name as string) as product_category_name,
    cast(product_category_name_english as string) as product_category_name_english
    
from {{ source("ecommerce_data", "olist_product_category_name") }}
