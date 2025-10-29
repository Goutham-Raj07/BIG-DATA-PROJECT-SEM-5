# Databricks notebook source
from pyspark.sql.functions import to_timestamp

# ---- Paths ----
raw_path = "s3://customer-seg-project/data.csv"
bronze_path = "s3://customer-seg-project/bronze/"
bronze_csv_path = "s3://customer-seg-project/exports/bronze_csv/"

# ---- Load raw CSV with inferred schema ----
df_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_path)
)

# ---- Convert event_time to timestamp ----
df_bronze = df_bronze.withColumn("event_time", to_timestamp("event_time"))

# ---- Save Bronze as Parquet ----
df_bronze.write.format("parquet").mode("overwrite").save(bronze_path)

# ---- Export CSV for validation/dashboard ----
df_bronze.write.option("header", "true").mode("overwrite").csv(bronze_csv_path)

display(df_bronze.limit(10))
