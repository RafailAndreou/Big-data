# filename: Q5_df_parquet.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Start Spark session
spark = SparkSession.builder.appName("Q5_df_parquet").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q5_df_parquet_{job_id}"

# Load trip data from Parquet
trip_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2024")

# Load taxi zone lookup table (CSV)
zone_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

# Join pickup and dropoff zones
trip_with_zones = trip_df \
    .join(zone_df.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Zone", "PickupZone"), on="PULocationID") \
    .join(zone_df.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Zone", "DropoffZone"), on="DOLocationID")

# Filter out trips where pickup and dropoff zones are the same
filtered = trip_with_zones.filter(col("PickupZone") != col("DropoffZone"))

# Count and sort the most frequent zone pairs
result = filtered.groupBy("PickupZone", "DropoffZone") \
                 .count() \
                 .withColumnRenamed("count", "TotalTrips") \
                 .orderBy(desc("TotalTrips"))

result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)
print(f"Output written to: {output_path}")
