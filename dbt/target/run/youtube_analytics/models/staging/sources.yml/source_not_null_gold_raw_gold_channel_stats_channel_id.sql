
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select channel_id
from "awsdatacatalog"."youtube_analytics_dev"."gold_channel_stats"
where channel_id is null



  
  
      
    ) dbt_internal_test