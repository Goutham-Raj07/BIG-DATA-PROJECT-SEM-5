# Databricks notebook source
df_bronze = spark.read.format("parquet").load("s3://customer-seg-project/bronze/")
df_silver = spark.read.format("delta").load("s3://customer-seg-project/silver_delta/")
df_gold   = spark.read.format("delta").load("s3://customer-seg-project/gold_delta/")

df_bronze.coalesce(1).write.option("header","true").mode("overwrite").csv("s3://customer-seg-project/exports/bronze_csv/")
df_silver.coalesce(1).write.option("header","true").mode("overwrite").csv("s3://customer-seg-project/exports/silver_csv/")
df_gold.coalesce(1).write.option("header","true").mode("overwrite").csv("s3://customer-seg-project/exports/gold_csv/")
