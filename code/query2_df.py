
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, max as spark_max, round
from pyspark.sql.types import DoubleType
import math

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of Earth in kilometers
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

haversine_udf = udf(haversine, DoubleType())

spark = SparkSession.builder.appName("Query2 - DF").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2015")

df = df.filter(
    (col("pickup_latitude") != 0) & (col("pickup_longitude") != 0) &
    (col("dropoff_latitude") != 0) & (col("dropoff_longitude") != 0)
)

df = df.withColumn("distance", haversine_udf(
    col("pickup_latitude"),
    col("pickup_longitude"),
    col("dropoff_latitude"),
    col("dropoff_longitude")
))

df = df.withColumn("duration", 
    (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60.0
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("VendorID").orderBy(col("distance").desc())

df_with_rank = df.withColumn("rank", row_number().over(windowSpec)).filter("rank = 1")

result = df_with_rank.select(
    "VendorID",
    round("distance", 2).alias("Max_Haversine_Distance_km"),
    round("duration", 2).alias("Duration_min")
)

result.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "hdfs://hdfs-namenode:9000/user/randreou/project_outputs/Q2_df"
)
