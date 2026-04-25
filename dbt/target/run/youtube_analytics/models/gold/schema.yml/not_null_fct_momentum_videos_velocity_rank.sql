
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select velocity_rank
from "awsdatacatalog"."dbt_gold"."fct_momentum_videos"
where velocity_rank is null



  
  
      
    ) dbt_internal_test