from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("Convert CSV to Parquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Input file paths
base_input_path = "hdfs://hdfs-namenode:9000/data/"
base_output_path = "hdfs://hdfs-namenode:9000/user/randreou/data/parquet/"

files = [
    "taxi_zone_lookup.csv",
    "yellow_tripdata_2015.csv",
    "yellow_tripdata_2024.csv"
]

# Convert each file to Parquet
for file in files:
    input_path = base_input_path + file
    output_path = base_output_path + file.replace(".csv", "")
    
    print(f"🔄 Processing {file}...")

    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Saved to {output_path}")

print("🎉 All files converted to Parquet.")
