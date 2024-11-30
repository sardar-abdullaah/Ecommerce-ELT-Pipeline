{{ config(materialized="table") }}
{{ config(schema="data_target") }}

select * from {{ ref("stg_sellers") }}
