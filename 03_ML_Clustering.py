# Databricks notebook source
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

gold_path = "s3://customer-seg-project/gold_delta/"
df_gold = spark.read.format("delta").load(gold_path)

numeric_cols = [
    "total_spent",
    "avg_transaction_value",
    "fraud_rate",
    "total_transactions",
    "unique_channels"
]
df_ml = df_gold.na.fill({c: 0 for c in numeric_cols})

feature_cols = [
    "total_spent",
    "avg_transaction_value",
    "fraud_rate",
    "total_transactions",
    "unique_channels"
]
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw"
)
df_vector = assembler.transform(df_ml)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=False
)
scaler_model = scaler.fit(df_vector)
df_scaled = scaler_model.transform(df_vector)

kmeans = KMeans(
    featuresCol="features",
    predictionCol="customer_segment",
    k=4,
    seed=42
)
model = kmeans.fit(df_scaled)
df_clusters = model.transform(df_scaled)

columns_to_keep = df_gold.columns + ["features", "customer_segment"]
df_clusters = df_clusters.select(*columns_to_keep)

df_clusters.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("`final-dataset`")

display(df_clusters.limit(10))