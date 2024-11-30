{{ config(materialized="table") }}
{{ config(schema="data_target") }}

select
    ro.order_id,
    ro.customer_id,
    ro.order_purchase_timestamp,
    roi.product_id,
    rop.payment_value,
    ro.order_status

from {{ ref("stg_orders") }} ro
join {{ ref("stg_order_items") }} roi 
    on ro.order_id = roi.order_id
join {{ ref("stg_order_payments") }} rop 
    on ro.order_id = rop.order_id
