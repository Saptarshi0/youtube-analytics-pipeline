
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        momentum_label as value_field,
        count(*) as n_records

    from "awsdatacatalog"."dbt_gold"."fct_momentum_videos"
    group by momentum_label

)

select *
from all_values
where value_field not in (
    'accelerating','growing','flat','declining'
)



  
  
      
    ) dbt_internal_test