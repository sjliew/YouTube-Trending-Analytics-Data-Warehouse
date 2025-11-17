from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, sha2, monotonically_increasing_id

spark = SparkSession.builder.appName("dim_channel_etl").getOrCreate()

database_name = "youtube_raw_db"

# Dynamically discover all video tables
table_list = [t.name for t in spark.catalog.listTables(database_name) if "video" in t.name.lower()]

if not table_list:
    raise ValueError(f"❌ No video tables found in database '{database_name}'")

print(f"📌 Detected video tables for channel dimension: {table_list}")

# Union all video tables
dfs = [spark.table(f"{database_name}.{table}") for table in table_list]
raw_df = dfs[0]
for df in dfs[1:]:
    raw_df = raw_df.unionByName(df, allowMissingColumns=True)

# Build channel dimension
dim_channel_df = (
    raw_df
        .select(trim(col("channel_title")).alias("channel_title"))
        .dropna()
        .dropDuplicates(["channel_title"])
)

# Add deterministic hashed natural key (useful for warehouse joins)
dim_channel_df = dim_channel_df.withColumn("channel_id", sha2(col("channel_title"), 256))

# Add surrogate key
dim_channel_df = dim_channel_df.withColumn("channel_sk", monotonically_increasing_id())

output_path = "s3://youtube-analytics-sj/staging/dim_channel/"

(
    dim_channel_df
        .select("channel_sk", "channel_id", "channel_title")
        .write.mode("overwrite")
        .parquet(output_path)
)

print("🎉 dim_channel ETL complete!")
