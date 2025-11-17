from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, min as spark_min, max as spark_max

spark = SparkSession.builder.appName("fact_trending_duration_etl").getOrCreate()

input_path = "s3://youtube-analytics-sj/staging/fact_video_stats/"
output_path = "s3://youtube-analytics-sj/staging/fact_trending_duration/"

df = spark.read.parquet(input_path)

fact_trending_duration_df = (df
    .groupBy("video_id", "country_code")
    .agg(
        countDistinct("date").alias("days_trending"),
        spark_min("date").alias("first_trending_date"),
        spark_max("date").alias("last_trending_date"),
    )
)

fact_trending_duration_df.write.mode("overwrite").parquet(output_path)

print("fact_trending_duration ETL complete.")
