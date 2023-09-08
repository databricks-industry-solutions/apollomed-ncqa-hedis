# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Generate Sample Data (100k Members)
# MAGIC - Assume all members have same eligibility
# MAGIC - Randomly draw sex at birth, race / ethnicity variables. Each cateogry has equal probability.
# MAGIC - Conform to poorly-controlled HBD [NCQA reported benchmarks](https://www.ncqa.org/hedis/measures/comprehensive-diabetes-care/) assuming Medicaid HMO

# COMMAND ----------

SAMPLE_SIZE = 100000
SEED_VALUE = 0
CDC_RACE_VARIABLES = [
    "2106-3",
    "2054-5",
    "1002-5",
    "2028-9",
    "2076-8",
    "OTH",
    "ASKU",
    "UNK"
]
CDC_ETHNICITY_VARIABLES = [
    "2135-2",
    "2186-5",
    "ASKU",
    "UNK"
]

# COMMAND ----------

import json
with open("data/HBD_example_1.json", "r") as f:
    hba1c_poorly_controlled = json.load(f)
with open("data/HBD_example_2.json", "r") as f:
    hba1c_well_controlled = json.load(f)

# COMMAND ----------

import random
import copy
random.seed(SEED_VALUE)
def gen_sample():
    rand_val = random.random()
    if rand_val >= 0.42:
        member_data = copy.deepcopy(hba1c_well_controlled)
    elif rand_val < 0.42:
        member_data = copy.deepcopy(hba1c_poorly_controlled)
    member_data['sex'] = random.choice(['M', 'F'])
    member_data["race1"] = random.choice(CDC_RACE_VARIABLES)
    member_data["ethn_res"] = random.choice(CDC_ETHNICITY_VARIABLES)
    member_data["my"] = "2023" # must be a string
    return member_data

# COMMAND ----------

data = []
for i in range(SAMPLE_SIZE):
    data.append(json.dumps(gen_sample()))

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
spark = SparkSession.builder.appName('gen-sample-data').getOrCreate()
input_df = spark.sparkContext.parallelize(data).map(Row).toDF(["chedispy_input"]).withColumn("member_id", F.expr("uuid()")).select("member_id", "chedispy_input")
input_df.persist()
