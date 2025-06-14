# filename: Q4_df_parquet.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col

# Initialize Spark session
spark = SparkSession.builder.appName("Q4_df_parquet").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q4_df_parquet_{job_id}"

# Load Parquet file
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2024")

# Extract hour and filter for 23:00–07:00 time range
filtered = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
             .filter((col("pickup_hour") >= 23) | (col("pickup_hour") < 7))

# Group by VendorID and count trips
result = filtered.groupBy("VendorID") \
                 .count() \
                 .withColumnRenamed("count", "TotalTrips") \
                 .orderBy(col("TotalTrips").desc())

# Show and save results
result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)

print(f"Output written to: {output_path}")
