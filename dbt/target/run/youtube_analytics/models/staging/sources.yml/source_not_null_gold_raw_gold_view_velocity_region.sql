
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region
from "awsdatacatalog"."youtube_analytics_dev"."gold_view_velocity"
where region is null



  
  
      
    ) dbt_internal_test