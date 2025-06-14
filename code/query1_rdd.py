
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query1 - RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2015")

filtered = df.rdd \
    .map(lambda row: (int(row["tpep_pickup_datetime"].hour), (row["pickup_latitude"], row["pickup_longitude"], 1))) \
    .filter(lambda x: x[1][0] != 0.0 and x[1][1] != 0.0)

aggregated = filtered.reduceByKey(lambda a, b: (
    a[0] + b[0],
    a[1] + b[1],
    a[2] + b[2]
))

averaged = aggregated.map(lambda x: (x[0], round(x[1][0] / x[1][2], 6), round(x[1][1] / x[1][2], 6))) \
    .sortBy(lambda x: x[0])

# Save result to HDFS
output_path = "hdfs://hdfs-namenode:9000/user/randreou/project_outputs/Q1_rdd"
averaged.map(lambda x: f"{x[0]:02d},{x[1]},{x[2]}").coalesce(1).saveAsTextFile(output_path)
