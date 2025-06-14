# filename: Q5_df_csv.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Start Spark session
spark = SparkSession.builder.appName("Q5_df_csv").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q5_df_csv_{job_id}"

# Read trip data
trip_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

# Read taxi zone lookup table
zone_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

# Join pickup location IDs with zone names
trip_with_pickup = trip_df.join(zone_df.withColumnRenamed("LocationID", "PULocationID")
                                .withColumnRenamed("Zone", "PickupZone"), on="PULocationID")

# Join dropoff location IDs with zone names
trip_full = trip_with_pickup.join(zone_df.withColumnRenamed("LocationID", "DOLocationID")
                                  .withColumnRenamed("Zone", "DropoffZone"), on="DOLocationID")

# Filter out same-zone trips and group by Pickup → Dropoff zone
result = trip_full.filter(col("PickupZone") != col("DropoffZone")) \
                  .groupBy("PickupZone", "DropoffZone") \
                  .count() \
                  .withColumnRenamed("count", "TotalTrips") \
                  .orderBy(desc("TotalTrips"))

# Show and save
result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)
print(f"Output written to: {output_path}")
