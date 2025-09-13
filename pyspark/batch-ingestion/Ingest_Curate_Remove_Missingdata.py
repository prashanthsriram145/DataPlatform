from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV Data Curation") \
    .getOrCreate()

# Path to CSV file

input_path = "./data/input/ncr_ride_bookings.csv"

# Read CSV file
df = spark.read.csv(input_path, header=True, inferSchema=True)
df.show(truncate=False)
# Mandatory fields
# mandatory_fields = ["customer_id", "name", "email"]

# # Filter out rows where any mandatory field is null or empty
# curated_df = df.filter(
#     " AND ".join([f"{col_name} IS NOT NULL AND {col_name} != ''" for col_name in mandatory_fields])
# )

# Show curated data
# curated_df.show(truncate=False)

# Optionally write curated data back to ADLS Gen2 in parquet/csv format
# output_path = "abfss://container@storageaccount.dfs.core.windows.net/output/curated_data"
# curated_df.write.mode("overwrite").parquet(output_path)

spark.stop()
