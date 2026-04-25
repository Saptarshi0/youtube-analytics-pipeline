with cat_base as (
    select
        category_id,
        category_name,
        region,
        snapshot_date,
        video_count,
        total_views,
        total_likes,
        total_comments,
        avg_view_count,
        avg_engagement_rate,
        avg_duration_seconds,
        max_view_count,
        top_video_title,
        top_video_id
    from "awsdatacatalog"."dbt_staging"."stg_category_stats"
),

global_totals as (
    select
        category_id,
        snapshot_date,
        sum(video_count)             as global_video_count,
        sum(total_views)             as global_total_views,
        avg(avg_engagement_rate)     as global_avg_engagement
    from cat_base
    group by category_id, snapshot_date
),

regional_ranked as (
    select
        c.*,
        g.global_video_count,
        g.global_total_views,
        g.global_avg_engagement,
        rank() over (
            partition by c.region, c.snapshot_date
            order by c.total_views desc
        ) as category_rank_in_region
    from cat_base c
    join global_totals g
      on c.category_id   = g.category_id
     and c.snapshot_date = g.snapshot_date
)

select
    category_id,
    category_name,
    region,
    snapshot_date,
    category_rank_in_region,
    video_count,
    total_views,
    total_likes,
    total_comments,
    round(avg_view_count, 0)                  as avg_view_count,
    round(avg_engagement_rate * 100, 4)       as avg_engagement_rate_pct,
    round(avg_duration_seconds / 60.0, 1)     as avg_duration_minutes,
    max_view_count,
    top_video_title,
    top_video_id,
    global_video_count,
    global_total_views,
    round(global_avg_engagement * 100, 4)     as global_avg_engagement_pct,
    round(
        total_views * 100.0 / nullif(global_total_views, 0),
        2
    ) as region_view_share_pct
from regional_ranked
order by region, snapshot_date, category_rank_in_region