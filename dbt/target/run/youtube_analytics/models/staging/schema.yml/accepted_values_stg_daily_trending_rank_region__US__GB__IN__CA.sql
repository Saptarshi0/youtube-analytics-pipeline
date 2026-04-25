
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        region as value_field,
        count(*) as n_records

    from "awsdatacatalog"."dbt_staging"."stg_daily_trending_rank"
    group by region

)

select *
from all_values
where value_field not in (
    'US','GB','IN','CA'
)



  
  
      
    ) dbt_internal_test