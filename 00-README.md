![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

## Scalable, In-House Quality Measurement with an NCQA-Certified Engine
Quality measurement is a core component of value-based care contracting. Unfortunately, data regarding quality performance are typically delivered via diversely-formatted Excel spreadsheets, acquired from various payors in non-automated ways. A recent industry shift has led to a consolidation of the data sources underlying quality metrics in *curated delta lakes*, often with comparatively stable ingestions on industry standard formats. For the first time, *it is possible for many organizations to measure their quality performance daily, at scale, using internal data sources.* The ApolloMed Velox Quality engine facilitates this use case, allowing certified HEDIS measures to be executed in parallel.

In this solution accelerator, we illustrate how to use the ApolloMed Velox Quality engine to:
1. `01-member-sample`: Generate a [HEDIS-certified HBD](https://www.ncqa.org/hedis/measures/comprehensive-diabetes-care/) measure result based on a simple JSON data source
2. `02-scalable-processing`: Apply the quality engine to a larger record-set and analyze the results
___

![image](https://velox-public-image.s3.amazonaws.com/quality_engine_diagram.png)

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| Library Name   | Library License | Library License URL |  Library Source URL |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| Apache Spark     | Apache License 2.0 | 	https://github.com/apache/spark/blob/master/LICENSE | 	https://github.com/apache/spark/tree/master/python/pyspark |
| `chedispy` | [Proprietary License - Apollomed Medical Holdings Inc.](https://www.apollomed.net/) | | |


## Chedispy Installation

`chedispy` is ApolloMed's certified, 