
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select snapshot_date
from "awsdatacatalog"."dbt_gold"."fct_category_performance"
where snapshot_date is null



  
  
      
    ) dbt_internal_test