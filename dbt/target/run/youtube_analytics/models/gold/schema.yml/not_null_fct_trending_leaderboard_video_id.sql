
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select video_id
from "awsdatacatalog"."dbt_gold"."fct_trending_leaderboard"
where video_id is null



  
  
      
    ) dbt_internal_test