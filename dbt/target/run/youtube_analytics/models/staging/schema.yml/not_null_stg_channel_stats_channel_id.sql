
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select channel_id
from "awsdatacatalog"."dbt_staging"."stg_channel_stats"
where channel_id is null



  
  
      
    ) dbt_internal_test