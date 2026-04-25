
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region
from "awsdatacatalog"."dbt_gold"."fct_trending_leaderboard"
where region is null



  
  
      
    ) dbt_internal_test