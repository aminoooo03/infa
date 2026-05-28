{{ config(materialized='table') }}
WITH clicks AS (
  SELECT product_id, click_count 
  FROM {{ ref('popular_products') }}
),
orders AS (
  SELECT product_id, SUM(quantity) as total_orders 
  FROM `my-project-lab1-497719.raw.orders` 
  GROUP BY product_id
)
SELECT 
  c.product_id, 
  c.click_count*0.7 + COALESCE(o.total_orders,0)*0.3 as score
FROM clicks c 
LEFT JOIN orders o ON c.product_id = o.product_id
ORDER BY score DESC 
LIMIT 10
