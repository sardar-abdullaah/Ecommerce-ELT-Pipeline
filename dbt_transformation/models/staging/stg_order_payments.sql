{{ config(materialized="table") }}
{{ config(schema="data_staging") }}


select
    cast(order_id as string) as order_id,
    cast(payment_sequential as int64) as payment_sequential,
    cast(payment_type as string) as payment_type,
    cast(payment_installments as int64) as payment_installments,
    cast(payment_value as numeric) as payment_value

from {{ source("ecommerce_data", "olist_order_payments") }}
