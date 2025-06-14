# filename: Q3_sql_parquet.py

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("Q3_sql_parquet").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q3_sql_parquet_{job_id}"

# Load Parquet files and create temporary views
trips_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2024")
zones_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/taxi_zone_lookup")

trips_df.createOrReplaceTempView("trips")
zones_df.createOrReplaceTempView("zones")

# SQL query to compute same-borough trips and count per borough
query = """
SELECT z1.Borough AS Borough, COUNT(*) AS TotalTrips
FROM trips t
LEFT JOIN zones z1 ON t.PULocationID = z1.LocationID
LEFT JOIN zones z2 ON t.DOLocationID = z2.LocationID
WHERE z1.Borough = z2.Borough
GROUP BY z1.Borough
ORDER BY TotalTrips DESC
"""

result = spark.sql(query)

# Show and save results
result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)

print(f"Done. Output written to: {output_path}")
