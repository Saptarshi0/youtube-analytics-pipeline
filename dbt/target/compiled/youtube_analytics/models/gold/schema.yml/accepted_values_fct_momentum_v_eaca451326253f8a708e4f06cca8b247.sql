
    
    

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


