# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Aggregate Analysis
# MAGIC In this notebook, we run `read-sample` which duplicates our original member from `01-member-sample` with some randomized attributes. We then illustrate how to run the `HBD` via a Spark UDF. Using a UDF facilitates scaling to as many measure, measure year combinations as necessary. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a new Database for Test Data

# COMMAND ----------

# DBTITLE 0,Create a new Database for Test Data
db = "chedispy_results"
spark.sql(f"DROP SCHEMA IF EXISTS {db} CASCADE")
spark.sql(f"CREATE SCHEMA {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read sample data

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StringType

data = []
with open("./data/sample_data.ndjson", "r") as fi:
    df = spark.createDataFrame(
        [Row(chedispy_input=line.replace("\n", "")) for line in fi.readlines()]
    ).withColumn("member_id", F.expr("uuid()"))
df.display(100)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Register Spark UDF and Run the HEDIS engine

# COMMAND ----------

# DBTITLE 0,Register Spark UDF and Run the HEDIS engine
import importlib, json
from pyspark.sql.types import StringType
import pandas

if importlib.util.find_spec("chedispy") is None:
    """
    If you DO NOT have the chedispy library yet, the following block runs to allow you to see sample output
    """
    print(
        "ApolloMed's HEDIS engine is not installed on this cluster. Examples below will proceed using sample output data provided in Github"
    )
    with open("./data/unparsed_results.txt", "r") as fi:
        data = [line.split("|") for line in fi.readlines()]
    df2 = spark.createDataFrame(
        [
            Row(chedispy_input=line[0], member_id=line[1], unparsed_result=line[2])
            for line in data
        ]
    )
else:
    """
    If you do have the library, the following code can be used to run the HEDIS engine
    """
    import json
    from chedispy.load_engine import load_engine

    engine = load_engine(measure="HBD")

    def apply_chedispy(member_data):
        try:
            member_dict = json.loads(member_data)
            res = engine.get_measure(member=member_dict)
            return json.dumps(res)
        except Exception as e:
            return '{"error" : "' + str(e) + '"}'

    apply_chedispy_udf = F.udf(apply_chedispy, StringType())
    df2 = df.withColumn("unparsed_result", apply_chedispy_udf(F.col("chedispy_input")))
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Parse the Result

# COMMAND ----------

# DBTITLE 0,Parse the Result
output_schema = F.schema_of_json(df2.select("unparsed_result").first()[0])
df3 = df2.withColumn("chedispy_output", F.from_json("unparsed_result", output_schema))
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode the Results 
# MAGIC `chedispy` returns an array of results for two possible reasons:
# MAGIC - A measure may have multiple submeasures (measure_id). For instance, poorly controlled and well-controlled
# MAGIC - A member may have result for multiple payers. For example many measures are reportable to both Medicaid and Medicare.

# COMMAND ----------

# DBTITLE 0,Explode the Results (Member may have result for Multiple Payers)
df4 = df3.withColumn("exploded", F.explode(F.col("chedispy_output"))).select(
    "member_id", "exploded.*"
)
df4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run the UDF Over Input Dataframe

# COMMAND ----------

df4.write.mode("overwrite").saveAsTable(f"{db}.member_measure")

# COMMAND ----------

spark.sql(f"SELECT * FROM {db}.member_measure").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Report With SQL
# MAGIC
# MAGIC With results written to the `member_measure` table, it's simple to aggregate in SQL, or any downstream BI reporting tool.
# MAGIC
# MAGIC #### Notes
# MAGIC - Race and Ethnicity variables do not correspond to realistic distribution
# MAGIC - Since a single member may be reportable to multiple payers, *group by's are not mutually exclusive or summable*

# COMMAND ----------

spark.sql(
    f"""
SELECT 
  measure_id, payer,
  SUM(CAST(num.value AS INT)) / SUM(CAST(denom.value AS INT)) AS performance_rate
FROM {db}.member_measure
WHERE stratified_report.report = true
GROUP BY measure_id, payer
ORDER BY measure_id, payer, SUM(CAST(num.value AS INT)) / SUM(CAST(denom.value AS INT)) ASC
"""
).display()

# COMMAND ----------

spark.sql(
    f"""
SELECT 
  measure_id, payer, stratified_report.race,
  SUM(CAST(num.value AS INT)) / SUM(CAST(denom.value AS INT)) AS performance_rate
FROM {db}.member_measure
WHERE stratified_report.report = true
GROUP BY measure_id, payer, stratified_report.race
ORDER BY measure_id, payer, SUM(CAST(num.value AS INT)) / SUM(CAST(denom.value AS INT)) ASC
"""
).display()
