
from pyspark.sql import SparkSession
import math

# Haversine distance calculation
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of Earth in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

spark = SparkSession.builder.appName("Query2 - RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Load the data
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2015")

rdd = df.rdd     .filter(lambda row: row["pickup_latitude"] != 0 and row["pickup_longitude"] != 0 and
                        row["dropoff_latitude"] != 0 and row["dropoff_longitude"] != 0)     .map(lambda row: (
        row["VendorID"],
        haversine(row["pickup_latitude"], row["pickup_longitude"],
                  row["dropoff_latitude"], row["dropoff_longitude"]),
        (row["tpep_dropoff_datetime"].timestamp() - row["tpep_pickup_datetime"].timestamp()) / 60.0
    ))     .filter(lambda x: x[2] > 0)

# Compute max distance per VendorID
result = rdd.map(lambda x: (x[0], (x[1], x[2])))     .reduceByKey(lambda a, b: a if a[0] > b[0] else b)     .map(lambda x: f"{x[0]},{round(x[1][0], 2)},{round(x[1][1], 2)}")     .coalesce(1)

# Save to HDFS
result.saveAsTextFile("hdfs://hdfs-namenode:9000/user/randreou/project_outputs/Q2_rdd")
