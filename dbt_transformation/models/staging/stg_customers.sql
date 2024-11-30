{{ config(materialized="table") }}
{{ config(schema="data_staging") }}


select
    cast(customer_id as string) as customer_id,
    cast(customer_unique_id as string) as customer_unique_id,
    cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
    cast(customer_city as string) as customer_city,
    cast(customer_state as string) as customer_state
    
from {{ source("ecommerce_data", "olist_customers") }}
