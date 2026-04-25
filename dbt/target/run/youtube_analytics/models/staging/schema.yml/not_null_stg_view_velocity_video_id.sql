
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select video_id
from "awsdatacatalog"."dbt_staging"."stg_view_velocity"
where video_id is null



  
  
      
    ) dbt_internal_test