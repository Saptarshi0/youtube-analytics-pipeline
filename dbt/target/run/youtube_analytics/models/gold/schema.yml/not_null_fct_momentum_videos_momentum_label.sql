
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select momentum_label
from "awsdatacatalog"."dbt_gold"."fct_momentum_videos"
where momentum_label is null



  
  
      
    ) dbt_internal_test