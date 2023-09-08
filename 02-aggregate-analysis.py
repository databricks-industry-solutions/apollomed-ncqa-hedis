# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Aggregate Analysis
# MAGIC In this notebook, we run `03-generate-data` which duplicates our original member from `01-member-sample` with some randomized attributes. We then illustrate how to run the `HBD` via a Spark UDF. Using a UDF facilitates scaling to as many measure, measure year combinations as necessary. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create a new Database for Test Data

# COMMAND ----------

db = "chedispy_results"
spark.sql(f"DROP SCHEMA IF EXISTS {db} CASCADE")
spark.sql(f"CREATE SCHEMA {db}")

# COMMAND ----------

# MAGIC %run "./03-generate-sample-data"

# COMMAND ----------

input_df.display(100)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Register UDF's
# MAGIC We register two UDF's:
# MAGIC - `apply_chedispy`: Load JSON data from string to a Python object, runs the HBD engine, and writes back to JSON.
# MAGIC - `unpack_result`: Unpack relevant fields for aggregate analysis into a Spark Struct.

# COMMAND ----------

# Import the HBDEngine
from chedispy.hbd import HBDEngine
from chedispy.utils import load_dmap_default
dmap = load_dmap_default()
engine = HBDEngine(dmap)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StructType, StringType, StructField, IntegerType
import json

def apply_chedispy(member_data):
    member_dict = json.loads(member_data)
    res = engine.get_measure(
        member=member_dict
    )
    return json.dumps(res)

def upack_result(res_str):
    res = json.loads(res_str)
    if res:
        res = [
            {"denom": int(pred["denom"]["value"]),
            "num": int(pred["num"]["value"]),
            "submeasure_code": pred["measure_id"],
            "payer": pred["payer"],
            **pred["stratified_report"]
            }
            for pred in res
        ]
    return res

schema = ArrayType(
    StructType([
        StructField("num", IntegerType(), True),
        StructField("denom", IntegerType(), True),
        StructField("submeasure_code", StringType(), True),
        StructField("payer", StringType(), True),
        StructField("report", BooleanType(), True),
        StructField("sex", StringType(), True),
        StructField("race", StringType(), True),
        StructField("ethnicity", StringType(), True),
        StructField("race_ds", StringType(), True),
        StructField("ethnicity_ds", StringType(), True)])
)
apply_chedispy_udf = F.udf(apply_chedispy, StringType()) # StructType() not possible given dynamic schema
extract_res_udf = F.udf(upack_result, schema) # StructType() not possible given dynamic schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run the UDF Over Input Dataframe

# COMMAND ----------

res_df = input_df.withColumn(
    "chedispy_result_json", 
    apply_chedispy_udf(
        input_df["chedispy_input"]
    )
)
res_df = res_df.withColumn(
    "result",
    F.explode(extract_res_udf(
        res_df["chedispy_result_json"]
    ))
).select("member_id", "result.*")
res_df.write.mode("overwrite").saveAsTable(f"{db}.member_measure")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Report With SQL
# MAGIC
# MAGIC With results written to the `member_measure` table, it's simple to aggregate in SQL, or any BI reporting tool.
# MAGIC
# MAGIC #### Notes
# MAGIC - Race and Ethnicity variables do not correspond to realistic distribution
# MAGIC - Since a single member may be reportable to multiple payers, group by's are not mutually exclusive or summable

# COMMAND ----------

spark.sql(f"""
SELECT 
  submeasure_code, payer,
  SUM(num) / SUM(denom) AS performance_rate
FROM {db}.member_measure
WHERE report = true
GROUP BY submeasure_code, payer
ORDER BY submeasure_code, payer, SUM(num) / SUM(denom) ASC
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT 
  submeasure_code, payer, race,
  SUM(num) / SUM(denom) AS performance_rate
FROM {db}.member_measure
WHERE report = true
GROUP BY submeasure_code, payer, race
ORDER BY submeasure_code, payer, SUM(num) / SUM(denom) ASC
""").display()
