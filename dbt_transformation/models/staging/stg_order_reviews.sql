{{ config(materialized="table") }}
{{ config(schema="data_staging") }}

SELECT 
    cast(review_id AS STRING) AS review_id,
    cast(order_id AS STRING) AS order_id,
    cast(review_score AS STRING) AS review_score,
    cast(review_comment_title AS STRING) AS review_comment_title,
    cast(review_comment_message AS STRING) AS review_comment_message,
    SAFE.parse_timestamp('%Y-%m-%d %H:%M:%S', review_creation_date) AS review_creation_date,
    SAFE.parse_timestamp('%Y-%m-%d %H:%M:%S', review_answer_timestamp) AS review_answer_timestamp

from {{ source("ecommerce_data", "olist_order_reviews") }}


