
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region
from "awsdatacatalog"."dbt_gold"."fct_category_performance"
where region is null



  
  
      
    ) dbt_internal_test