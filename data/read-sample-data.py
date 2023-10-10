# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Read Sample Data
# MAGIC Read 10k sample data. See `data/generate-sample-data.py` for details regarding how the sample was created.

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json

with open("./data/sample_data.ndjson", "r") as fi:
    data = fi.readlines()
    spark = SparkSession.builder.appName('read-sample-data').getOrCreate()
    input_df = spark.sparkContext.parallelize(data).map(Row).toDF(["chedispy_input"]).withColumn("member_id", F.expr("uuid()")).select("member_id", "chedispy_input")
    input_df.persist()
