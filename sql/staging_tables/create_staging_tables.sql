CREATE TABLE IF NOT EXISTS staging.dim_video (
    video_sk BIGINT,
    video_id VARCHAR(64),
    title VARCHAR(500),
    channel_title VARCHAR(200),
    publish_time TIMESTAMP,
    tags VARCHAR(5000),
    description VARCHAR(5000)
);

CREATE TABLE IF NOT EXISTS staging.dim_channel (
    channel_sk BIGINT,
    channel_id VARCHAR(256),
    channel_title VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS staging.dim_category (
    category_sk BIGINT,
    category_id INT
);

CREATE TABLE IF NOT EXISTS staging.dim_country (
    country_sk BIGINT,
    country_code VARCHAR(10),
    country_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.fact_video_stats (
    video_id VARCHAR(64),
    country_code VARCHAR(10),
    date DATE,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT
);
