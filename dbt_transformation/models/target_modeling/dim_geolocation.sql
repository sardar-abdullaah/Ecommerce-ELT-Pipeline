{{ config(materialized="table") }}
{{ config(schema="data_target") }}


SELECT
    *
FROM {{ ref( "stg_geolocation") }}