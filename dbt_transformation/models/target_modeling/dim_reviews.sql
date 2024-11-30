{{ config(materialized="table") }}
{{ config(schema="data_target") }}

select
    review_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    order_id

from {{ ref("stg_order_reviews") }}
