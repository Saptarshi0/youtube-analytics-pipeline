create or replace view
    "awsdatacatalog"."dbt_staging"."stg_category_stats"
  as
    with source as (
    select * from "awsdatacatalog"."youtube_analytics_dev"."gold_category_stats"
),

enriched as (
    select
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
        end                                          as category_name,
        region,
        snapshot_date,
        coalesce(video_count, 0)                     as video_count,
        coalesce(total_views, 0)                     as total_views,
        coalesce(total_likes, 0)                     as total_likes,
        coalesce(total_comments, 0)                  as total_comments,
        coalesce(avg_view_count, 0.0)                as avg_view_count,
        coalesce(avg_like_count, 0.0)                as avg_like_count,
        coalesce(avg_comment_count, 0.0)             as avg_comment_count,
        coalesce(avg_engagement_rate, 0.0)           as avg_engagement_rate,
        coalesce(avg_duration_seconds, 0.0)          as avg_duration_seconds,
        coalesce(max_view_count, 0)                  as max_view_count,
        coalesce(min_view_count, 0)                  as min_view_count,
        top_video_title,
        top_video_id,
        _gold_processed_at
    from source
)

select * from enriched
