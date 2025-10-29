# Databricks notebook source
from pyspark.sql import functions as F

silver_path = "s3://customer-seg-project/silver_delta/"
gold_path = "s3://customer-seg-project/gold_delta/"
gold_csv_path = "s3://customer-seg-project/exports/gold_csv/"

df_silver = spark.read.format("delta").load(silver_path)

# Aggregate Metrics
df_gold = (
    df_silver.groupBy("customer_type", "customer_age_group", "city", "merchant_cat")
    .agg(
        F.sum("amount").alias("total_spent"),
        F.count("*").alias("total_transactions"),        
        F.countDistinct("channel").alias("unique_channels"),
        F.sum("label_fraud").alias("fraud_transactions")
    )
    .withColumn("avg_transaction_value", F.round(F.col("total_spent") / F.col("total_transactions"), 2)) 
    .withColumn("fraud_rate", F.round(F.col("fraud_transactions") / F.col("total_transactions"), 4))
)

# Save Gold Delta
df_gold.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(gold_path)

# Export CSV
df_gold.write.option("header","true").mode("overwrite").csv(gold_csv_path)

display(df_gold.limit(10))
