DROP TABLE IF EXISTS marts.trending_overview;

CREATE TABLE marts.trending_overview AS
WITH daily_stats AS (
    SELECT
        date,
        COUNT(DISTINCT video_sk) AS trending_videos,
        COUNT(DISTINCT channel_sk) AS active_channels,
        COUNT(DISTINCT country_sk) AS active_countries,
        AVG(engagement_rate) AS avg_engagement_rate,
        AVG(like_ratio) AS avg_like_ratio
    FROM analytics.fact_video_performance
    GROUP BY date
),

country_rank AS (
    SELECT
        date,
        country_sk,
        RANK() OVER (
            PARTITION BY date
            ORDER BY COUNT(*) DESC
        ) AS country_rank
    FROM analytics.fact_video_performance
    GROUP BY date, country_sk
)

SELECT
    ds.*,
    cr.country_sk AS top_country_sk
FROM daily_stats ds
LEFT JOIN country_rank cr
    ON ds.date = cr.date AND cr.country_rank = 1
ORDER BY ds.date;
