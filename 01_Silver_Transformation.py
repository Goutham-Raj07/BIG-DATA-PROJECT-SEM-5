# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, regexp_replace

# Paths
bronze_path = "s3://customer-seg-project/bronze/"
silver_path = "s3://customer-seg-project/silver_delta/"
silver_csv_path = "s3://customer-seg-project/exports/silver_csv/"

# Load Bronze Parquet
df_bronze = spark.read.format("parquet").load(bronze_path)

# Transform Data
def bool_to_int(column):
    return when(col(column) == "True", 1).when(col(column) == "False", 0).otherwise(None)

df_silver = (
    df_bronze
    .withColumn("amount", regexp_replace(col("amount"), ",", "").cast("double"))
    .withColumn("is_international", bool_to_int("is_international"))
    .withColumn("is_chip", bool_to_int("is_chip"))
    .withColumn("is_contactless", bool_to_int("is_contactless"))
    .withColumn("label_fraud", bool_to_int("label_fraud"))
)

# Save Silver Delta
df_silver.write.format("delta").mode("overwrite").save(silver_path)

# Export CSV
df_silver.write.option("header","true").mode("overwrite").csv(silver_csv_path)

display(df_silver.limit(10))
