import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_timestamp, monotonically_increasing_id

spark = SparkSession.builder.appName("dim_video_etl").getOrCreate()

database_name = "youtube_raw_db"

# Dynamically load all country video tables
table_list = [t.name for t in spark.catalog.listTables(database_name) if "video" in t.name]

dfs = [spark.table(f"{database_name}.{table}") for table in table_list]
raw_videos_df = dfs[0]
for df in dfs[1:]:
    raw_videos_df = raw_videos_df.unionByName(df)

dim_video_df = raw_videos_df.select(
    "video_id",
    trim(col("title")).alias("title"),
    trim(col("channel_title")).alias("channel_title"),
    trim(col("publish_time")).alias("publish_time"),
    trim(col("tags")).alias("tags"),
    trim(col("description")).alias("description")
).dropDuplicates(["video_id"])

# Convert publish_time to timestamp (sample format: 2017-11-13T17:13:01.000Z)
dim_video_df = dim_video_df.withColumn(
    "publish_time", to_timestamp(col("publish_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

dim_video_df = dim_video_df.withColumn("video_sk", monotonically_increasing_id())

output_path = "s3://youtube-analytics-sj/staging/dim_video/"

dim_video_df.write.mode("overwrite").parquet(output_path)

print("dim_video ETL complete.")
