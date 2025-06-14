# filename: Q6_df_parquet.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Initialize Spark session
spark = SparkSession.builder.appName("Q6_df_parquet").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "randreou"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/project_outputs/Q6_df_parquet_{job_id}"

# Load Parquet dataset
trips_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/yellow_tripdata_2024")

# Load taxi zone lookup table
zones_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/randreou/data/parquet/taxi_zone_lookup")



# Join to get Borough from PULocationID
joined_df = trips_df.join(zones_df, trips_df.PULocationID == zones_df.LocationID, "left")

# Group by Borough and sum revenue columns
result = joined_df.groupBy("Borough").agg(
    spark_sum("fare_amount").alias("Fare"),
    spark_sum("tip_amount").alias("Tips"),
    spark_sum("tolls_amount").alias("Tolls"),
    spark_sum("extra").alias("Extras"),
    spark_sum("mta_tax").alias("MTA_Tax"),
    spark_sum("congestion_surcharge").alias("Congestion"),
    spark_sum("airport_fee").alias("Airport_Fee"),
    spark_sum("total_amount").alias("Total_Revenue")
).orderBy(col("Total_Revenue").desc())

# Show and export results
result.show(truncate=False)
result.coalesce(1).write.mode("overwrite").csv(output_path)
print(f"Output written to: {output_path}")
