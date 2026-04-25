with velocity_base as (
    select *
    from {{ ref('stg_view_velocity') }}
    where velocity_pct is not null
),

ranked as (
    select
        *,
        rank() over (
            partition by region, snapshot_date
            order by velocity_pct desc
        ) as velocity_rank
    from velocity_base
)

select
    video_id,
    title,
    channel_id,
    channel_title,
    category_id,
    region,
    snapshot_date,
    velocity_rank,
    view_count,
    view_count_delta,
    round(velocity_pct, 2)              as velocity_pct,
    days_since_prev_snapshot,
    momentum_label,
    like_count,
    comment_count,
    round(engagement_rate * 100, 4)    as engagement_rate_pct
from ranked
where velocity_rank <= 20
order by region, snapshot_date, velocity_rank
