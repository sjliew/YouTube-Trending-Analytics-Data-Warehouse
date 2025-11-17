from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("dim_country_etl").getOrCreate()

database_name = "youtube_raw_db"
table_list = [t.name for t in spark.catalog.listTables(database_name) if t.name.endswith("videos")]

countries = sorted(set([tbl.split("_")[0].upper() for tbl in table_list]))

schema = StructType([
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), True)
])

rows = [(c, c) for c in countries]
dim_country_df = spark.createDataFrame(rows, schema)

output_path = "s3://youtube-analytics-sj/staging/dim_country/"
dim_country_df.write.mode("overwrite").parquet(output_path)

print("dim_country ETL complete.")
