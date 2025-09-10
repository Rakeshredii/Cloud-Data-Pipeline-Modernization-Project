
# Databricks notebook: transformation.py
# Joins and aggregates Silver into Gold models.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum
spark = SparkSession.builder.getOrCreate()

a = spark.read.format("delta").load("/mnt/silver/source_a")
b = spark.read.format("delta").load("/mnt/silver/source_b")
c = spark.read.format("delta").load("/mnt/silver/source_c")

# Example model: user-level activity summary
a_grp = a.groupBy("user_id").agg(_sum("amount").alias("total_amount"),
                                 count("*").alias("events"))
b_grp = b.groupBy("user_id").agg(count("*").alias("api_events"))

joined = (a_grp.join(b_grp, "user_id", "left")
               .join(c, "user_id", "left"))

joined.write.mode("overwrite").format("delta").save("/mnt/gold/user_activity")
