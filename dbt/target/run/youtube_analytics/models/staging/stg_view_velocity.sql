create or replace view
    "awsdatacatalog"."dbt_staging"."stg_view_velocity"
  as
    with source as (
    select * from "awsdatacatalog"."youtube_analytics_dev"."gold_view_velocity"
)

select
    video_id,
    title,
    channel_id,
    channel_title,
    category_id,
    region,
    snapshot_date,
    coalesce(view_count, 0)               as view_count,
    coalesce(like_count, 0)               as like_count,
    coalesce(comment_count, 0)            as comment_count,
    coalesce(engagement_rate, 0.0)        as engagement_rate,
    view_count_delta,
    velocity_pct,
    days_since_prev_snapshot,
    case
        when velocity_pct is null then null
        when velocity_pct > 20    then 'accelerating'
        when velocity_pct > 0     then 'growing'
        when velocity_pct = 0     then 'flat'
        else                           'declining'
    end as momentum_label,
    _gold_processed_at
from source
