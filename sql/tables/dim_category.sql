CREATE EXTERNAL TABLE spectrum.dim_category_ext (
    category_sk BIGINT,
    category_id INT
)
STORED AS PARQUET
LOCATION 's3://youtube-analytics-sj/staging/dim_category/';
