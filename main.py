from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, percentile_approx, avg
from delta import configure_spark_with_delta_pip

# Initialize Spark session with Delta Lake support
builder = SparkSession.builder \
    .appName("NYC Taxi Lakehouse") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# File paths
default_base = "/user/ali"
input_path = f"{default_base}/taxi_tripdata.csv"
delta_raw_path = f"{default_base}/delta/nyc_taxi_raw"
delta_clean_path = f"{default_base}/delta/nyc_taxi_clean"
delta_result_path = f"{default_base}/delta/nyc_taxi_summary"

# --- : Load raw CSV ---
taxi_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)
print(f"Raw data count: {taxi_df.count()}")
taxi_df.show(5, truncate=False)

taxi_df.write.format("delta").mode("overwrite").save(delta_raw_path)

# --- : Cleaning ---
taxi_raw = spark.read.format("delta").load(delta_raw_path)
cleaned = taxi_raw \
    .filter((col("fare_amount") > 0) & (col("trip_distance") > 0)) \
    .dropDuplicates()
# Compute percentiles and filter outliers
percentiles = cleaned.select(
    percentile_approx(col("fare_amount"), 0.01).alias("p1"),
    percentile_approx(col("fare_amount"), 0.99).alias("p99")
).first()
p1, p99 = percentiles["p1"], percentiles["p99"]
cleaned = cleaned.filter((col("fare_amount") >= p1) & (col("fare_amount") <= p99))
# Add trip_type
cleaned = cleaned.withColumn(
    "trip_type",
    when(col("trip_distance") < 1, "short")
    .when(col("trip_distance") < 5, "medium")
    .otherwise("long")
)
print(f"Cleaned data count: {cleaned.count()}")
cleaned.show(5, truncate=False)

cleaned.write.format("delta").mode("overwrite").save(delta_clean_path)

# --- gold: Summary ---
clean_df = spark.read.format("delta").load(delta_clean_path)
summary = clean_df.groupBy("PULocationID", "DOLocationID").agg(
    avg(col("fare_amount")).alias("avg_fare")
)
print("Top 10 average fares per route:")
summary.orderBy(col("avg_fare").desc()).show(10, truncate=False)

summary.write.format("delta").mode("overwrite").save(delta_result_path)

print("Lakehouse pipeline complete:")
print(f"Raw Delta at: {delta_raw_path}")
print(f"Clean Delta at: {delta_clean_path}")
print(f"Summary Delta at: {delta_result_path}")
