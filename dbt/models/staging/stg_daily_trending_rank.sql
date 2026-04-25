with source as (
    select * from {{ source('gold_raw', 'gold_daily_trending_rank') }}
),

add_category_name as (
    select
        video_id,
        title,
        channel_id,
        channel_title,
        category_id,
        case category_id
            when 1  then 'Film & Animation'
            when 2  then 'Autos & Vehicles'
            when 10 then 'Music'
            when 15 then 'Pets & Animals'
            when 17 then 'Sports'
            when 18 then 'Short Movies'
            when 19 then 'Travel & Events'
            when 20 then 'Gaming'
            when 21 then 'Videoblogging'
            when 22 then 'People & Blogs'
            when 23 then 'Comedy'
            when 24 then 'Entertainment'
            when 25 then 'News & Politics'
            when 26 then 'Howto & Style'
            when 27 then 'Education'
            when 28 then 'Science & Technology'
            when 29 then 'Nonprofits & Activism'
            else        'Unknown (' || cast(category_id as varchar) || ')'
        end                                                  as category_name,
        region,
        snapshot_date,
        trending_rank,
        coalesce(view_count, 0)                              as view_count,
        coalesce(like_count, 0)                              as like_count,
        coalesce(comment_count, 0)                           as comment_count,
        coalesce(engagement_rate, 0.0)                       as engagement_rate,
        coalesce(like_to_view_ratio, 0.0)                    as like_to_view_ratio,
        duration_seconds,
        tags_count,
        published_date,
        _gold_processed_at
    from source
)

select * from add_category_name
