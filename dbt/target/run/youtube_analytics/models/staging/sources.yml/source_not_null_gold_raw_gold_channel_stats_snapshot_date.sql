
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select snapshot_date
from "awsdatacatalog"."youtube_analytics_dev"."gold_channel_stats"
where snapshot_date is null



  
  
      
    ) dbt_internal_test