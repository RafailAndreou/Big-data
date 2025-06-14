# filename: Q3_df_parquet.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# initialize Spark
spark = SparkSession.builder.appName("Q3_df_parquet").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"  
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q3_df_parquet_{job_id}"

# Load Parquet files
trips_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2024")
zones_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/taxi_zone_lookup")

# Create aliases for clarity
pickup_zones = zones_df.selectExpr("LocationID as PULocationID", "Borough as PUBorough")
dropoff_zones = zones_df.selectExpr("LocationID as DOLocationID", "Borough as DOBorough")

# Join to get boroughs
with_boroughs = trips_df \
    .join(pickup_zones, on="PULocationID", how="left") \
    .join(dropoff_zones, on="DOLocationID", how="left")

# Keep only rows where pickup and dropoff borough match
same_borough = with_boroughs.filter(col("PUBorough") == col("DOBorough"))

# Count trips per borough
result = same_borough.groupBy("PUBorough").count() \
    .withColumnRenamed("PUBorough", "Borough") \
    .withColumnRenamed("count", "TotalTrips") \
    .orderBy(col("TotalTrips").desc())

# Show and save
result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)

print(f" Done. Output written to: {output_path}")
