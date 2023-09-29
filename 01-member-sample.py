# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Running a Quality Measure
# MAGIC In this notebook, we illustrate how to run the Apollomed quality engine for a single measure (`HBD`) on a single member. Next in `02-aggregate-analysis`, we upsample this member's data and show how to use the quality engine results for aggregate analyses.

# COMMAND ----------

# MAGIC %md
# MAGIC # A Sample Member
# MAGIC In `data`, we have included a member sample `HBD_example_1.json` to illustrate correct formatting. For an exhaustive formatting guide, review our [public docs](https://ameh.notion.site/ApolloMed-Quality-Engine-Documentation-3250d28383fa4a3a9cf7eab6b41296ce?pvs=4).
# MAGIC
# MAGIC ### Patient Demographics:
# MAGIC - **Sex**: Male
# MAGIC - **Date of Birth**: 1957-02-01
# MAGIC - **Age**: 65 (as of the date of presentation)
# MAGIC - **Race**: (Asian per [CDC Codset](https://phinvads.cdc.gov/vads/ViewValueSet.action?id=67D34BBC-617F-DD11-B38D-00188B398520))
# MAGIC - **Ethnicity**: Patient refused to provide ethnicity (ASKU)
# MAGIC
# MAGIC ### Insurance Details (As of February 2023):
# MAGIC Let's assume this quality measurement occurred in February 2023. The member had previously been enrolled solely with Medicare. Starting this month, they are dual-eligible under Medicare and Medicaid. Notice that in our JSON sample, we assume continuous enrollment through the end of 2023 by setting the enrollment end date to `2023-12-31`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Clinical Presentation:
# MAGIC On `2022-12-31`, a 65-year-old Asian male presented to the emergency department (`POS|23`) with symptoms of hypoglycemia. Further assessment confirmed hypoglycemia and surfaced undiagnosed type 2 diabetes mellitus (`E11.9`). An HbA1C test was performed and logged in the EHR (`SNOWMED|451061000124104`). The resutls indicate that the patient's diabetes is poorly controlled (`HbA1C >9%`).
# MAGIC
# MAGIC ### Follow-up and Management:
# MAGIC Upon discharge from the ED, the patient had a subsequent visit (`2023-01-07`) with a primary care physician (PCP) who further characterized the patient's conditions
# MAGIC - Type 2 diabetes mellitus (`E11.9`)
# MAGIC - Essential (primary) hypertension (`I10`)
# MAGIC - Mood disorder, unspecified (`F39`)
# MAGIC
# MAGIC ### New Medication Regimen:
# MAGIC The PCP prescribed a 90 day supply of insulin glargine (`NDC|0088221905`) which the patient picked up from a retail pharmacy on `2023-01-10`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Representing As JSON
# MAGIC There are a few nuances to point out as we represent this member with JSON.
# MAGIC
# MAGIC - **File Type**: A record's origin is indicated by the `"file"` key, in this case `"visit"` (claims), `"proc"` (an EHR procedure feed), and `"pharm"` (retail pharmacy).
# MAGIC - **Supplemental Data**: Health plans are historically claims-centric. From their perspective, data which does not originate in claims is "supplemental data".
# MAGIC - **Assuming Continuous Enrollment**: Most healthcare quality teams want to *project their performance*. If we're in February 2023, we assume continuous enrollment through the end of 2023, i.e. 2023-12-31. Without this assumption, members will not be flagged as denominator eligible.
# MAGIC - **Provider Specialties**: Provider types and specialties relevant to measure logic are represented at the claim level. `"PresProv"` indicates a provider with prescribing privileges. `"PCP"` indciates "Primary Care Provider", a category typically defined in contract between payers and providers.

# COMMAND ----------

import json
from pprint import pprint
with open("data/HBD_example_1.json", "r") as f:
    member_data = json.load(f)
pprint(member_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running the Certified HEDIS Engine (chedispy)
# MAGIC Once the data is formatted correctly, running quality measures is simple. Let's run [Hemoglobin A1c Control for Patients With Diabetes (HBD)](https://www.ncqa.org/wp-content/uploads/2021/12/HEDIS-MY-2022-Measure-Descriptions.pdf). For more in depth logic see [Page 146](https://www.cms.gov/files/document/2023-qrs-technical-specifications.pdf) of the technical specification. 
# MAGIC
# MAGIC #### What do we expect?
# MAGIC Given continuous enrollment in Medicare and sufficient claims records indicating diabetes, the member should be eligible for the denominator. The procedure record with `SNOWMED|451061000124104` indicates that the member's diabetes is "poorly controlled." This type of "procedure" record would originate from an EHR data feed, or perhaps an ADT.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### TODO - explain logistics of installing chedispy
# MAGIC
# MAGIC - import sourcedefender
# MAGIC - if chedispy is not installed, import shell of chedispy if and warn user

# COMMAND ----------

# MAGIC %pip install sourcedefender

# COMMAND ----------

# check that chedispy is installed and import
import sourcedefender, importlib, json
res = None
if importlib.util.find_spec('chedispy') is None:  
  """
  If you DO NOT have the chedispy library yet, the following block runs to allow you to see sample output 
  """
  print("Apollomed's HEDIS engine is not installed on this cluster. Examples below will proceed using sample output data provided in Github")
  res =  json.load(open('./data/HBD_result_1.json', 'r'))
else:
  """
  If you do have the library, the following code can be used to run the HEDIS engine
  """
  from chedispy.hbd import HBDEngine
  from chedispy.utils import load_dmap_default
  dmap = load_dmap_default()
  engine = HBDEngine(dmap)
  # Assess HBD logic for measurement year 2023
  member_data["my"] = "2023"
  res = engine.get_measure(
    member=member_data
  )
  

# COMMAND ----------

pprint(res)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpreting Results
# MAGIC #### Multiple Results
# MAGIC In this case, `chedispy` returns an array with four results, each corresponding to a reporting requirement for health plans. There are two dimensions driving this result...
# MAGIC
# MAGIC - **Submeasures**: HEDIS separates HBD into two submeasures (HBD1, HBD2), quantifying the population with well-controlled HbA1c (<8.0%), and poorly controlled (>9.0%). As we expected, a positive HBD2 numerator indicates the member has "poorly controlled" diabetes.
# MAGIC - **Reportable Payers**: Given continous enrollment in *Medicare and Medicaid* (MMP), a health plan would be required to *report this result to both agencies* (MCD, MCR)

# COMMAND ----------

print(f"There are {len(res)} reportable payer / submeasure combinations...")
for i, val in enumerate(res, 1):
    print(f'Result #{i}, Submeasure={val["measure_id"]}, Payer={val["payer"]}, Numerator={val["num"]["value"]}, Denominator={val["denom"]["value"]}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Concrete Evidence
# MAGIC `chedispy` gives detail regarding *why* a given member meets denominator and numerator criteria. Inclusion "events" for both the denominator and numerator criteria include claims detail or logical hints to facilitate root-cause tracing.
# MAGIC
# MAGIC ```
# MAGIC 'measure_id': 'HBD2',
# MAGIC 'denom': {
# MAGIC     'value': true,
# MAGIC     'evidence': {
# MAGIC         'has_inclusion_events': {
# MAGIC             'included': [
# MAGIC                         {'DOS': ['2022-12-31', '2023-01-07'],
# MAGIC                         'claim_id': ['1','3'],
# MAGIC                         'desc': 'Diabetes in vsnms'},
# MAGIC                         {'DOS': ['2022-12-31'],
# MAGIC                         'claim_id': ['1'],
# MAGIC                         'desc': "['ED'] in vsnms'},
# MAGIC                         {'DOS': ['2023-01-07'],
# MAGIC                         'claim_id': ['3'],
# MAGIC                         'desc': "['Outpatient'] in vsnms'}]}
# MAGIC         }
# MAGIC     }
# MAGIC },
# MAGIC 'num': {
# MAGIC     'value': true,
# MAGIC     'evidence': {
# MAGIC         'excluded': [],
# MAGIC         'included': [{'DOS': [''],
# MAGIC                         'claim_id': [''],
# MAGIC                         'desc': 'Has diabetes indicators'}]
# MAGIC         }
# MAGIC }
# MAGIC ```
# MAGIC #### Stratified Reporting
# MAGIC The first step to unwinding the influence of race throughout the healthcare system is to measure its impact. To this end, various payors now require a stratified reporting of quality measures by race / enthnicity dimensions (see [pg 38](https://www.cms.gov/files/document/2023-qrs-technical-specifications.pdf)). These required dimensions are available for aggregation in `stratified_report`.
# MAGIC
# MAGIC ```
# MAGIC  'stratified_report': {'ethnicity': '3', # Asked but No Answer
# MAGIC                        'ethnicity_ds': '1', # Direct (CMS Databases, State Databases, Health Plan...)
# MAGIC                        'race': '4', # Asian
# MAGIC                        'race_ds': '1', # Direct (CMS Databases, State Databases, Health Plan...)
# MAGIC                        'report': True,
# MAGIC                        'sex': 'M'}
# MAGIC ```

# COMMAND ----------

# The MCD / HBD2 result
pprint(res[1])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## A Good News Story
# MAGIC By `2023-04-12`, a follow up laboratory test indicated an improved Hemoglobin A1c level of 7.5% (LOINC code: `4548-4`). The medication regimen has led to improvement. Note however, in the absense of an improved HbA1C value, the quality management team would likely prompt clinical teams to reach out to the patient if they have not already.

# COMMAND ----------

import json
from pprint import pprint
with open("data/HBD_example_2.json", "r") as f:
    member_data = json.load(f)
pprint(member_data)

# COMMAND ----------

res = None
if importlib.util.find_spec('chedispy') is None:  
  """
  If you DO NOT have the chedispy library yet, the following block runs to allow you to see sample output 
  """
  print("Apollomed's HEDIS engine is not installed on this cluster. Examples below will proceed using sample output data provided in Github")
  res =  json.load(open('./data/HBD_result_2.json', 'r'))
else:
  """
  If you do have the library, the following code can be used to run the HEDIS engine
  """

  # Assess HBD logic for measurement year 2023
  member_data["my"] = "2023"
  res = engine.get_measure(
  member=member_data
)
  
# Assess HBD logic for measurement year 2023
pprint(res)

# COMMAND ----------

print(f"There are {len(res)} reportable payer / submeasure combinations...")
for i, val in enumerate(res, 1):
    print(f'Result #{i}, Submeasure={val["measure_id"]}, Payer={val["payer"]}, Numerator={val["num"]["value"]}, Denominator={val["denom"]["value"]}')

# COMMAND ----------

# MAGIC %md # Scaling for Large Data
# MAGIC
# MAGIC Building a Spark UDF for running at scale

# COMMAND ----------

if importlib.util.find_spec('chedispy') is None:
  dbutils.notebook.exit("Stopping notebook because chedispy is not available. See results above for data reference")

# COMMAND ----------

# DBTITLE 1,Read in sample input data
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

# DBTITLE 1,UDF to run the HEDIS engine
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

# DBTITLE 1,UDF to parse output JSON data
import pyspark.sql.functions as F
output_schema = F.schema_of_json(df2.select('unparsed_result').first()[0])
df3 = df2.withColumn("chedispy_output", F.from_json("unparsed_result", output_schema))
df3.show()
