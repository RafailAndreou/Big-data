# filename: Q3_sql_csv.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("Q3_sql_csv").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q3_sql_csv_{job_id}"

# Define schema for yellow_tripdata_2024.csv
trip_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# Read CSV with schema and fail-safe mode
trips_df = spark.read.option("header", True)\
    .schema(trip_schema)\
    .option("mode", "DROPMALFORMED")\
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

zones_df = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

trips_df.createOrReplaceTempView("trips")
zones_df.createOrReplaceTempView("zones")

result = spark.sql("""
    SELECT z1.Borough AS Borough, COUNT(*) AS TotalTrips
    FROM trips t
    LEFT JOIN zones z1 ON t.PULocationID = z1.LocationID
    LEFT JOIN zones z2 ON t.DOLocationID = z2.LocationID
    WHERE z1.Borough = z2.Borough AND z1.Borough IS NOT NULL
    GROUP BY z1.Borough
    ORDER BY TotalTrips DESC
""")

result.show()
result.coalesce(1).write.mode("overwrite").csv(output_path)

print(f"Done. Output written to: {output_path}")
