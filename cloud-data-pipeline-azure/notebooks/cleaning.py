
# Databricks notebook: cleaning.py
# Cleans Bronze into Silver with validation checks.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit
spark = SparkSession.builder.getOrCreate()

df_a = spark.read.format("delta").load("/mnt/bronze/source_a")
df_b = spark.read.format("delta").load("/mnt/bronze/source_b")
df_c = spark.read.format("delta").load("/mnt/bronze/source_c")

# Example validations: drop rows with null keys, de-duplicate
df_a_clean = df_a.dropna(subset=["event_id","user_id"]).dropDuplicates(["event_id"])
df_b_clean = df_b.dropna(subset=["api_event_id","user_id"]).dropDuplicates(["api_event_id"])
df_c_clean = df_c.dropna(subset=["user_id"]).dropDuplicates(["user_id"])

# Simple DQ summary counts
def dq_summary(df, name):
    total = df.count()
    null_user = df.filter(col("user_id").isNull()).count() if "user_id" in df.columns else 0
    return (name, total, null_user)

summaries = [dq_summary(df_a_clean,"source_a"),
             dq_summary(df_b_clean,"source_b"),
             dq_summary(df_c_clean,"source_c")]

# Persist Silver
df_a_clean.write.mode("overwrite").format("delta").save("/mnt/silver/source_a")
df_b_clean.write.mode("overwrite").format("delta").save("/mnt/silver/source_b")
df_c_clean.write.mode("overwrite").format("delta").save("/mnt/silver/source_c")
