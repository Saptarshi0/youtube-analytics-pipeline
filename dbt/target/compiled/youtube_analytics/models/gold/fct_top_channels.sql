with channel_base as (
    select
        channel_id,
        channel_title,
        region,
        snapshot_date,
        trending_video_count,
        total_views,
        total_likes,
        total_comments,
        avg_view_count,
        avg_engagement_rate,
        max_video_views
    from "awsdatacatalog"."dbt_staging"."stg_channel_stats"
),

ranked as (
    select
        *,
        rank() over (
            partition by region, snapshot_date
            order by total_views desc
        ) as channel_rank,
        total_views * 1.0 / nullif(
            sum(total_views) over (partition by region, snapshot_date),
            0
        ) as view_share_pct
    from channel_base
)

select
    channel_id,
    channel_title,
    region,
    snapshot_date,
    channel_rank,
    trending_video_count,
    total_views,
    total_likes,
    total_comments,
    round(avg_view_count, 0)             as avg_view_count,
    round(avg_engagement_rate * 100, 4)  as avg_engagement_rate_pct,
    max_video_views,
    round(view_share_pct * 100, 2)       as view_share_pct
from ranked
order by region, snapshot_date, channel_rank