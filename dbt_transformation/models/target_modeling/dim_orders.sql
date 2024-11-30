{{ config(materialized="table") }}
{{ config(schema="data_target") }}


SELECT 
    ro.order_id
    , ro.order_status
    , ro.order_purchase_timestamp
    , ro.order_approved_at
    , ro.order_delivered_carrier_date
    , ro.order_delivered_customer_date
    , ro.order_estimated_delivery_date
    , ro.customer_id
    , roi.shipping_limit_date
    , roi.order_item_id
    , roi.price
    , roi.freight_value
    , roi.seller_id AS seller_id
    , rop.payment_sequential
    , rop.payment_type
    , rop.payment_installments
    , rop.payment_value

FROM  {{ ref( "stg_order_items") }} roi 
    INNER JOIN {{ ref( "stg_orders") }} ro 
        ON roi.order_id = ro.order_id
    INNER JOIN {{ ref( "stg_order_payments") }} rop 
        ON ro.order_id = rop.order_id
