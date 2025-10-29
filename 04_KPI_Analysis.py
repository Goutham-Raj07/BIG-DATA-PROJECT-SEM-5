# Databricks notebook source
df_kpi = spark.read.table("workspace_gold_customer_segments")
df_kpi.createOrReplaceTempView("customer_segments")

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 1: Total Spend by Cluster**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cluster AS customer_segment,
# MAGIC        SUM(total_spent) AS total_spent,
# MAGIC        AVG(avg_transaction_value) AS avg_transaction_value
# MAGIC FROM customer_segments
# MAGIC GROUP BY cluster
# MAGIC ORDER BY total_spent DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC KPI 2: Fraud Rate by Merchant Category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT merchant_cat,
# MAGIC        ROUND(AVG(fraud_rate), 4) AS avg_fraud_rate
# MAGIC FROM customer_segments
# MAGIC GROUP BY merchant_cat
# MAGIC ORDER BY avg_fraud_rate DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC KPI 3: Total Transactions by City

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city,
# MAGIC        SUM(total_transactions) AS total_transactions
# MAGIC FROM customer_segments
# MAGIC GROUP BY city
# MAGIC ORDER BY total_transactions DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI 4: Top Merchant Category per Cluster**

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH agg AS (
# MAGIC   SELECT
# MAGIC     cluster AS customer_segment,
# MAGIC     merchant_cat,
# MAGIC     SUM(total_spent) AS total_spent
# MAGIC   FROM customer_segments
# MAGIC   GROUP BY cluster, merchant_cat
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_segment,
# MAGIC   merchant_cat,
# MAGIC   total_spent
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY customer_segment
# MAGIC       ORDER BY total_spent DESC
# MAGIC     ) AS rn
# MAGIC   FROM agg
# MAGIC )
# MAGIC WHERE rn = 1

# COMMAND ----------

# MAGIC %md
# MAGIC KPI 5: Total Spend by Age Group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_age_group,
# MAGIC        SUM(total_spent) AS total_spent,
# MAGIC        AVG(avg_transaction_value) AS avg_transaction_value
# MAGIC FROM customer_segments
# MAGIC GROUP BY customer_age_group
# MAGIC ORDER BY total_spent DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC KPI 6: Cluster Distribution by Age Group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   cluster AS customer_segment,
# MAGIC   customer_age_group,
# MAGIC   COUNT(*) AS customer_count
# MAGIC FROM customer_segments
# MAGIC GROUP BY cluster, customer_age_group
# MAGIC ORDER BY cluster, customer_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC KPI 7: Average Transaction Value by Age Group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_age_group,
# MAGIC        ROUND(AVG(avg_transaction_value),2) AS avg_transaction_value
# MAGIC FROM customer_segments
# MAGIC GROUP BY customer_age_group
# MAGIC ORDER BY avg_transaction_value DESC;
# MAGIC