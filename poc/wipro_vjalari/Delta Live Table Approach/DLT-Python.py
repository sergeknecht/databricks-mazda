# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

csv_path = "s3://mazda-bi20-data-nonprd/data/Bronze/tbcoc_product.csv"

# COMMAND ----------

@dlt.table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def python_tbcoc_product():
  return (spark.read.format("csv").load(csv_path))
