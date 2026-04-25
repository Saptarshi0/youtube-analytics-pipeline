
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select channel_rank
from "awsdatacatalog"."dbt_gold"."fct_top_channels"
where channel_rank is null



  
  
      
    ) dbt_internal_test