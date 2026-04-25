
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select video_id
from "awsdatacatalog"."youtube_analytics_dev"."gold_view_velocity"
where video_id is null



  
  
      
    ) dbt_internal_test