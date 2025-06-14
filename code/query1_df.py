from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg, round

spark = SparkSession.builder.appName("Query1 - DataFrame").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2015")

result = df \
    .filter((col("pickup_latitude") != 0) & (col("pickup_longitude") != 0)) \
    .withColumn("hour", hour("tpep_pickup_datetime")) \
    .groupBy("hour") \
    .agg(
        round(avg("pickup_latitude"), 6).alias("avg_lat"),
        round(avg("pickup_longitude"), 6).alias("avg_lon")
    ) \
    .orderBy("hour")

# Save result to HDFS
output_path = "hdfs://hdfs-namenode:9000/user/randreou/project_outputs/Q1_df"
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
