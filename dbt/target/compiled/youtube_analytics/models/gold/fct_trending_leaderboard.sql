with ranked as (
    select *
    from "awsdatacatalog"."dbt_staging"."stg_daily_trending_rank"
    where trending_rank <= 10
),

with_age as (
    select
        *,
        date_diff('day', published_date, snapshot_date) as days_to_trend
    from ranked
)

select
    video_id,
    title,
    channel_id,
    channel_title,
    category_id,
    category_name,
    region,
    snapshot_date,
    trending_rank,
    view_count,
    like_count,
    comment_count,
    engagement_rate,
    like_to_view_ratio,
    duration_seconds,
    tags_count,
    published_date,
    days_to_trend,
    ntile(5) over (
        partition by region, snapshot_date
        order by engagement_rate desc
    ) as engagement_quintile
from with_age
order by region, snapshot_date, trending_rank