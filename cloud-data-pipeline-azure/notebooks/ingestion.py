
# Databricks notebook: ingestion.py
# Reads raw data from various sources into Bronze layer.
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

source_a = spark.read.option("header", True).csv("dbfs:/FileStore/data/source_a.csv")
source_b = spark.read.json("dbfs:/FileStore/data/source_b.json")
source_c = spark.read.option("header", True).csv("dbfs:/FileStore/data/source_c.csv")

# Basic casting
from pyspark.sql.functions import col, to_timestamp
df_a = (source_a
        .withColumn("event_id", col("event_id").cast("int"))
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("event_dt", to_timestamp("event_dt"))
)
df_b = (source_b
        .withColumn("api_event_id", col("api_event_id").cast("int"))
        .withColumn("user_id", col("user_id").cast("int"))
        .withColumn("ts", to_timestamp("ts"))
)
df_c = (source_c
        .withColumn("user_id", col("user_id").cast("int"))
)

df_a.write.mode("overwrite").format("delta").save("/mnt/bronze/source_a")
df_b.write.mode("overwrite").format("delta").save("/mnt/bronze/source_b")
df_c.write.mode("overwrite").format("delta").save("/mnt/bronze/source_c")
