{{ config(materialized="table") }}
{{ config(schema="data_target") }}

select 
    opd.product_id, 
    pcnt.product_category_name_english

from {{ ref("stg_products") }} opd
join
    {{ ref("stg_product_category_name_translation") }} pcnt
    on opd.product_category_name = pcnt.product_category_name
