# filename: query1_df_udf.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, avg, udf
from pyspark.sql.types import IntegerType

# Create Spark session
spark = SparkSession.builder.appName("Query1 - DataFrame with UDF").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define UDF to safely extract the hour
@udf(returnType=IntegerType())
def safe_hour(ts):
    if ts is None:
        return None
    return ts.hour

# Read 2015 dataset from HDFS (Parquet)
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2015")

# Filter and process
result = df \
    .filter((col("pickup_latitude") != 0) & (col("pickup_longitude") != 0)) \
    .withColumn("hour", safe_hour(col("tpep_pickup_datetime"))) \
    .groupBy("hour") \
    .agg(
        round(avg("pickup_latitude"), 6).alias("avg_lat"),
        round(avg("pickup_longitude"), 6).alias("avg_lon")
    ) \
    .orderBy("hour")

# Save result to HDFS
output_path = "hdfs://hdfs-namenode:9000/user/randreou/project_outputs/Q1_df_udf"
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print(f"Output written to: {output_path}")
