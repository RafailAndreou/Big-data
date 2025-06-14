from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query2 - SQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2015")
df.createOrReplaceTempView("trips")

query = """
WITH filtered AS (
  SELECT *,
    radians(pickup_latitude) AS phi1,
    radians(dropoff_latitude) AS phi2,
    radians(dropoff_latitude - pickup_latitude) AS delta_phi,
    radians(dropoff_longitude - pickup_longitude) AS delta_lambda
  FROM trips
  WHERE pickup_latitude != 0 AND pickup_longitude != 0
    AND dropoff_latitude != 0 AND dropoff_longitude != 0
),
distance_calc AS (
  SELECT *,
    2 * 6371 * ASIN(SQRT(
      SIN(delta_phi / 2) * SIN(delta_phi / 2) +
      COS(phi1) * COS(phi2) *
      SIN(delta_lambda / 2) * SIN(delta_lambda / 2)
    )) AS distance,
    (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 60.0 AS duration
  FROM filtered
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY VendorID ORDER BY distance DESC) AS rnk
  FROM distance_calc
)
SELECT VendorID,
       ROUND(distance, 2) AS Max_Haversine_Distance_km,
       ROUND(duration, 2) AS Duration_min
FROM ranked
WHERE rnk = 1
"""

result = spark.sql(query)

result.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "hdfs://hdfs-namenode:9000/user/randreou/project_outputs/Q2_sql"
)
