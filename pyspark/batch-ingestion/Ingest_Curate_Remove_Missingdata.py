from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
from pyspark.sql.functions import col, lit, when
from pyspark.ml.feature import Imputer

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV Data Curation") \
    .getOrCreate()

# Path to CSV file

input_path = "./data/output/deduplicated_data/"

# Read CSV file
df = spark.read.parquet(input_path)
print("Count of records read from parquet after deduplication: ", df.count())

# List of columns to check, or use df.columns for all columns
columns = df.columns

df = df.withColumn("Customer Rating", col("Customer Rating").cast("float"))

# Replace "null" string with None (which becomes null in Spark DataFrame)
for c in columns:
    df = df.withColumn(c, when(col(c) == "null", None).otherwise(col(c)))

df.printSchema()
print(df.count())

# df.show(truncate=False)
# Mandatory fields
mandatory_fields = ["Payment Method","Customer Rating Imputed"]

# Defaulting to Cash if Payment Method is missing
df = df.na.fill("Cash", subset=["Payment Method"])

# Impute missing values in 'Customer Rating' column with mean or median
imputer = Imputer(
    inputCols=["Customer Rating"], 
    outputCols=["Customer Rating Imputed"]
).setStrategy("mean")  # or "median"

df = imputer.fit(df).transform(df)

# check if any nulls remain in the imputed column
df.select(columns).where(col("Customer Rating Imputed").isNull()).show()
print("Showing all imputed values")
df.show(truncate=False)


# # Filter out rows where any mandatory field is null or empty
curated_df = df.na.drop(subset=mandatory_fields)

# Show curated data
print(curated_df.count())
select_fields = mandatory_fields + ["Booking Value"]
curated_df.select(select_fields).show(truncate=False)

# Optionally write curated data back to ADLS Gen2 in parquet/csv format
output_path = "./data/output/curated_data"
curated_df.write.mode("overwrite").parquet(output_path)

spark.stop()
