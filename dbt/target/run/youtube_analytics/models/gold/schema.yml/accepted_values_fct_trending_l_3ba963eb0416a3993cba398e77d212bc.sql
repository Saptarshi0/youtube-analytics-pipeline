
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        engagement_quintile as value_field,
        count(*) as n_records

    from "awsdatacatalog"."dbt_gold"."fct_trending_leaderboard"
    group by engagement_quintile

)

select *
from all_values
where value_field not in (
    1,2,3,4,5
)



  
  
      
    ) dbt_internal_test