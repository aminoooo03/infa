
  
    

    create or replace table `my-project-lab1-497719`.`dwh`.`popular_products`
      
    
    

    
    OPTIONS()
    as (
      
SELECT product_id, COUNT(*) as click_count
FROM `my-project-lab1-497719.raw.clicks`
GROUP BY product_id
    );
  