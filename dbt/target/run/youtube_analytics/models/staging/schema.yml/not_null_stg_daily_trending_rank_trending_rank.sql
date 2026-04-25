
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trending_rank
from "awsdatacatalog"."dbt_staging"."stg_daily_trending_rank"
where trending_rank is null



  
  
      
    ) dbt_internal_test