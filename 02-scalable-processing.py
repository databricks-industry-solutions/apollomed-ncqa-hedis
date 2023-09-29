# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Aggregate Analysis
# MAGIC In this notebook, we run `read-sample` which duplicates our original member from `01-member-sample` with some randomized attributes. We then illustrate how to run the `HBD` via a Spark UDF. Using a UDF facilitates scaling to as many measure, measure year combinations as necessary. 

# COMMAND ----------

if importlib.util.find_spec('chedispy') is None:
  dbutils.notebook.exit("Stopping notebook because chedispy is not available. See results above for data reference")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create a new Database for Test Data

# COMMAND ----------

db = "chedispy_results"
spark.sql(f"DROP SCHEMA IF EXISTS {db} CASCADE")
spark.sql(f"CREATE SCHEMA {db}")

# COMMAND ----------

# DBTITLE 1,Read sample data
import os
from pyspark.sql.functions import col
from pyspark.sql.types import *
df = (spark.read.format("csv")
        .option("header",False)
        .option("sep","||") #dummy separator
        .load("file:///" + os.getcwd() + "/data/sample_data.ndjson")
).select(col("_c0").alias("chedispy_input")).withColumn("member_id", F.expr("uuid()"))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Register a Spark UDF to run HEDIS engine

# COMMAND ----------

# DBTITLE 1,Run the HEDIS engine
def apply_chedispy(member_data):
  try: 
    member_dict = json.loads(member_data)
    res = engine.get_measure(
        member=member_dict
    )
    return json.dumps(res)
  except Exception as e:
    return  "{\"error\" : \"" + str(e) + "\"}" 
  
apply_chedispy_udf = F.udf(apply_chedispy, StringType())
df2 = df.withColumn("unparsed_result", apply_chedispy_udf(col("chedispy_input")))
df2.show()

# COMMAND ----------

# DBTITLE 1,Parse the Result
import pyspark.sql.functions as F
output_schema = F.schema_of_json(df2.select('unparsed_result').first()[0])
df3 = df2.withColumn("chedispy_output", F.from_json("unparsed_result", output_schema))
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run the UDF Over Input Dataframe

# COMMAND ----------

df3.select("member_id", "chedispy_output.*")
res_df.write.mode("overwrite").saveAsTable(f"{db}.member_measure")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM chedispy_results.member_measure

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Report With SQL
# MAGIC
# MAGIC With results written to the `member_measure` table, it's simple to aggregate in SQL, or any downstream BI reporting tool.
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
