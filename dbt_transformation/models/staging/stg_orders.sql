{{ config(materialized="table") }}
{{ config(schema="data_staging") }}


select 
    cast(order_id as string) as order_id,
    cast(customer_id as string) as customer_id,
    cast(order_status as string) as order_status,
    parse_timestamp('%Y-%m-%d %H:%M:%S', order_purchase_timestamp) as order_purchase_timestamp,
    parse_timestamp('%Y-%m-%d %H:%M:%S', order_approved_at) as order_approved_at,
    parse_timestamp('%Y-%m-%d %H:%M:%S', order_delivered_carrier_date) as order_delivered_carrier_date,
    parse_timestamp('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) as order_delivered_customer_date,
    parse_timestamp('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date) as order_estimated_delivery_date
    
from {{ source("ecommerce_data", "olist_orders") }}