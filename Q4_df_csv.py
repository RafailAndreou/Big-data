# filename: Q4_df_csv.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col

# Initialize Spark
spark = SparkSession.builder.appName("Q4_df_csv").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q4_df_csv_{job_id}"

# Load CSV file with header and infer schema
df = spark.read.option("header", True).option("inferSchema", True).csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

# Extract hour and filter for night time (23:00–06:59)
filtered = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
             .filter((col("pickup_hour") >= 23) | (col("pickup_hour") < 7))

# Group by VendorID
result = filtered.groupBy("VendorID").count().withColumnRenamed("count", "TotalTrips").orderBy(col("TotalTrips").desc())

# Show and write
result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)

print(f"Output written to: {output_path}")

