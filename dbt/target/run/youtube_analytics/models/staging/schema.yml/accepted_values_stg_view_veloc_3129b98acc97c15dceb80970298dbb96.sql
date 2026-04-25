
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        momentum_label as value_field,
        count(*) as n_records

    from "awsdatacatalog"."dbt_staging"."stg_view_velocity"
    group by momentum_label

)

select *
from all_values
where value_field not in (
    'accelerating','growing','flat','declining','None'
)



  
  
      
    ) dbt_internal_test