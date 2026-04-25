
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select engagement_quintile
from "awsdatacatalog"."dbt_gold"."fct_trending_leaderboard"
where engagement_quintile is null



  
  
      
    ) dbt_internal_test