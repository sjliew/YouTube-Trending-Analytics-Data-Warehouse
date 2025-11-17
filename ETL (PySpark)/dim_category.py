from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("dim_category_etl").getOrCreate()

database_name = "youtube_raw_db"
table_list = [t.name for t in spark.catalog.listTables(database_name) if t.name.endswith("videos")]

dfs = [spark.table(f"{database_name}.{tbl}") for tbl in table_list]
df = dfs[0]
for d in dfs[1:]:
    df = df.unionByName(d)

dim_category_df = (df
    .select(col("category_id").cast("int"))
    .dropna()
    .dropDuplicates(["category_id"])
)

output_path = "s3://youtube-analytics-sj/staging/dim_category/"

dim_category_df.write.mode("overwrite").parquet(output_path)

print("dim_category ETL complete.")
