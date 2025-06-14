# filename: Q4_sql_parquet.py

from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("Q4_sql_parquet").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q4_sql_parquet_{job_id}"

# Load Parquet data and register as temporary view
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2024")
df.createOrReplaceTempView("trips")

# SQL query for filtering and grouping
query = """
SELECT VendorID, COUNT(*) AS TotalTrips
FROM (
    SELECT VendorID, HOUR(tpep_pickup_datetime) AS pickup_hour
    FROM trips
) AS sub
WHERE pickup_hour >= 23 OR pickup_hour < 7
GROUP BY VendorID
ORDER BY TotalTrips DESC
"""

# Execute and save
result = spark.sql(query)
result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)

print(f"Output written to: {output_path}")
