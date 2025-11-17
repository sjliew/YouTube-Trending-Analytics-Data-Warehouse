CREATE EXTERNAL TABLE spectrum.fact_video_stats_ext (
    video_id VARCHAR(64),
    country_code VARCHAR(10),
    date DATE,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT
)
STORED AS PARQUET
LOCATION 's3://youtube-analytics-sj/staging/fact_video_stats/';
