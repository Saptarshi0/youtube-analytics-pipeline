create or replace view
    "awsdatacatalog"."dbt_staging"."stg_channel_stats"
  as
    with source as (
    select * from "awsdatacatalog"."youtube_analytics_dev"."gold_channel_stats"
)

select
    channel_id,
    channel_title,
    region,
    snapshot_date,
    coalesce(trending_video_count, 0)     as trending_video_count,
    coalesce(total_views, 0)              as total_views,
    coalesce(total_likes, 0)              as total_likes,
    coalesce(total_comments, 0)           as total_comments,
    coalesce(avg_view_count, 0.0)         as avg_view_count,
    coalesce(avg_engagement_rate, 0.0)    as avg_engagement_rate,
    coalesce(max_video_views, 0)          as max_video_views,
    trending_video_titles,
    _gold_processed_at
from source
