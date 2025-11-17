from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit

spark = SparkSession.builder.appName("fact_video_stats_etl").getOrCreate()

database_name = "youtube_raw_db"
table_list = [t.name for t in spark.catalog.listTables(database_name) if t.name.endswith("videos")]

fact_df = None
for tbl in table_list:
    country = tbl.split("_")[0].upper()
    df = spark.table(f"{database_name}.{tbl}")

    tmp = (df
        .withColumn("date", to_date(col("trending_date"), "yy.dd.MM"))
        .withColumn("views", col("views").cast("bigint"))
        .withColumn("likes", col("likes").cast("bigint"))
        .withColumn("dislikes", col("dislikes").cast("bigint"))
        .withColumn("comment_count", col("comment_count").cast("bigint"))
        .withColumn("country_code", lit(country))
        .select("video_id", "country_code", "date", "views", "likes", "dislikes", "comment_count")
    )

    fact_df = tmp if fact_df is None else fact_df.unionByName(tmp)

fact_df = fact_df.dropna(subset=["video_id", "date"])

output_path = "s3://youtube-analytics-sj/staging/fact_video_stats/"
fact_df.write.mode("overwrite").parquet(output_path)

print("fact_video_stats ETL complete.")
