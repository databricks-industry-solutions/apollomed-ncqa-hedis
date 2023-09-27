![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

## Scalable, In-House Quality Measurement with an NCQA-Certified Engine
Quality measurement is a core component of value-based care contracting. Unfortunately, data regarding quality performance are typically delivered via diversely-formatted Excel spreadsheets, acquired from various payors in non-automated ways. A recent industry shift has led to a consolidation of the data sources underlying quality metrics in *curated delta lakes*, often with comparatively stable ingestions on industry standard formats. For the first time, *it is possible for many organizations to measure their quality performance daily, at scale, using internal data sources.* The ApolloMed Velox Quality engine facilitates this use case, allowing certified HEDIS measures to be executed in parallel with a simple to use python library, `chedispy` (certified-hedispy).

In this solution accelerator, we illustrate how to use the ApolloMed Velox Quality engine to:
1. `01-member-sample`: Generate a [HEDIS-certified HBD](https://www.ncqa.org/hedis/measures/comprehensive-diabetes-care/) measure result based on a simple JSON data source
2. `02-scalable-processing`: Apply the quality engine to a larger record-set and analyze the results
___

![image](https://velox-public-image.s3.amazonaws.com/quality_engine_diagram.png)

___
## Chedispy (Certified HEDIS® Measures)

### Installation
If you're interested in purchasing the Apollomed Velox Quality engine for use internally, please reach out to [da_sales@apollomed.net](mailto:da_sales@apollomed.net). If you have already contracted with ApolloMed, you will receive a python wheel file which can be installed as a [library on Databricks clusters](https://docs.databricks.com/en/libraries/cluster-libraries.html). All `chedispy` dependencies will be installed during wheel installation.

### Usage
Usage is simple:
1. Transform internal data sources `chedispy` input format (JSON). Find an exhaustive formatting guide [here](https://ameh.notion.site/ameh/ApolloMed-Quality-Engine-Documentation-3250d28383fa4a3a9cf7eab6b41296ce)
1. Import `sourcedefender` which is required to run the encrypted HEDIS® logic.
2. Import a specific measure engine and choose a value-set map.
3. Apply the `get_measure` method to member data.

```
import sourcedefender
from chedispy.hbd import HBDEngine
from chedispy.utils import load_dmap_default
dmap = load_dmap_default()
engine = HBDEngine(dmap)

# Assess HBD logic for measurement year 2023
member_data["my"] = "2023"
res = engine.get_measure(
    member=member_data
)
pprint(res)
```

### Data Sources
`member_data` is represented as JSON and is assumed to be a combination of the following sources. Find an exhaustive formatting guide [here](https://ameh.notion.site/ameh/ApolloMed-Quality-Engine-Documentation-3250d28383fa4a3a9cf7eab6b41296ce)

| Category                 | Description                                                                                                                         | Typical Data Origin                                                                                                                                                         |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Member Demographics      | Attributes which do not change, or which change slowly, e.g. date of birth, race/ethnicity, etc.                                    | Payer and Health system registration systems                                                                                                                                |
| Eligibility / Enrollment | Data representing a member's enrollment with a payer during a given month. Sometimes called a "member month", or "effective month". | Payer Enrollment Data                                                                                                                                                       |
| Visit                    | Visits correspond essentially to claims. They typically contain service dates, procedure codes, and associated diagnosis codes.     | Payer Claims                                                                                                                                                                |
| Procedure                | Procedures ordered and / or completed in health system or clinical environment, whether or not a claim was filed.                   | EHR Feed                                                                                                                                                                    |
| Pharmacy Retail          | Prescriptions picked up or received via mail from a retail pharmacy                                                                 | Retail Pharmacies, Payer Claims, or HIE                                                                                                                                     |
| Pharmacy Clinical        | Prescriptions dispensed and / or administered in a clinical environment.                                                            | EHR Feed                                                                                                                                                                    |
| Diagnosis                | Diagnoses codes assigned within an EHR. Note, these may have varied degrees of certainty.                                           | EHR Feed                                                                                                                                                                    |
| Observation              | Varied types of tests, labs and monitoring (e.g. vital signs)                                                                       | EHR Feed                                                                                                                                                                    |
| Lab                      | Lab results typically originating from a health system or independent lab vendor.                                                   | Lab vendor or HIE                                                                                                                                                           |

---

### License

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| Library Name   | Library License | Library License URL |  Library Source URL |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| Apache Spark     | Apache License 2.0 | 	https://github.com/apache/spark/blob/master/LICENSE | 	https://github.com/apache/spark/tree/master/python/pyspark |
| `chedispy` | [Proprietary License - Apollomed Medical Holdings Inc.](https://www.apollomed.net/) | Reach out to [da_sales@apollomed.net](mailto:da_sales@apollomed.net) | |

