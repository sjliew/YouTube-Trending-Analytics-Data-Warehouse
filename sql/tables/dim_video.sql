CREATE EXTERNAL TABLE spectrum.dim_video_ext (
    video_sk BIGINT,
    video_id VARCHAR(100),
    title VARCHAR(1000),
    channel_title VARCHAR(500),
    publish_time TIMESTAMP,
    tags VARCHAR(5000),
    description VARCHAR(10000)
)
STORED AS PARQUET
LOCATION 's3://youtube-analytics-sj/staging/dim_video/';
