CREATE EXTERNAL TABLE spectrum.dim_channel_ext (
    channel_sk BIGINT,
    channel_id VARCHAR(256),
    channel_title VARCHAR(500)
)
STORED AS PARQUET
LOCATION 's3://youtube-analytics-sj/staging/dim_channel/';
