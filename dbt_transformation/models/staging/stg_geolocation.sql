{{ config(materialized="table") }}
{{ config(schema="data_staging") }}


select 
    cast(geolocation_zip_code_prefix as string) as geolocation_zip_code_prefix,
    cast(geolocation_lat as string) as geolocation_lat,
    cast(geolocation_lng as string) as geolocation_lng,
    cast(geolocation_city as string) as geolocation_city,
    cast(geolocation_state as string) as geolocation_state
    
from {{ source("ecommerce_data", "olist_geolocation") }}