from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("fact_engagement_etl").getOrCreate()

input_path = "s3://youtube-analytics-sj/staging/fact_video_stats/"
output_path = "s3://youtube-analytics-sj/staging/fact_engagement/"

df = spark.read.parquet(input_path)

fact_engagement_df = (df
    .withColumn("like_rate",  when(col("views") > 0, col("likes") / col("views")))
    .withColumn("dislike_rate", when(col("views") > 0, col("dislikes") / col("views")))
    .withColumn("comment_rate", when(col("views") > 0, col("comment_count") / col("views")))
)

fact_engagement_df.write.mode("overwrite").parquet(output_path)

print("fact_engagement ETL complete.")
