{{ config(materialized='table') }}
SELECT product_id, COUNT(*) as click_count
FROM `my-project-lab1-497719.raw.clicks`
GROUP BY product_id
