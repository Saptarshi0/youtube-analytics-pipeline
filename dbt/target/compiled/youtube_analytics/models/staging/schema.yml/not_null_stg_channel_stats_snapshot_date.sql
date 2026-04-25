
    
    



select snapshot_date
from "awsdatacatalog"."dbt_staging"."stg_channel_stats"
where snapshot_date is null


