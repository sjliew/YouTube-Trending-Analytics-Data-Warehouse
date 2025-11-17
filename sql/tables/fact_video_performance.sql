DROP TABLE IF EXISTS analytics.fact_video_performance;

CREATE TABLE analytics.fact_video_performance AS
SELECT
    f.date,
    v.video_sk,
    c.channel_sk,
    cat.category_sk,
    co.country_sk,
    f.views,
    f.likes,
    f.dislikes,
    f.comment_count,

    (COALESCE(f.likes,0) + COALESCE(f.comment_count,0))::FLOAT
        / NULLIF(f.views,0) AS engagement_rate,

    f.likes::FLOAT / NULLIF((f.likes + f.dislikes),0) AS like_ratio

FROM staging.fact_video_stats f
LEFT JOIN staging.dim_video v ON f.video_id = v.video_id
LEFT JOIN staging.dim_channel c ON v.channel_title = c.channel_title
LEFT JOIN staging.dim_category cat ON 1 = 1   -- placeholder if needed
LEFT JOIN staging.dim_country co ON f.country_code = co.country_code;
