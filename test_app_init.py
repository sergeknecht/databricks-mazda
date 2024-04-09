# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from helpers.app_helper import init
from helpers.db_helper_delta import create_table, get_or_create_schema
from helpers.logger_helper import CATALOG, FQN, SCHEMA, TABLE_APPLICATION
from helpers.status_helper import create_status

# COMMAND ----------

scope = "dev"
result = create_status(scope=scope, status_message= "INIT", status_code= 201)
result["fqn"] = FQN.format(scope=scope)
result

# COMMAND ----------

df = spark.createDataFrame([result])
display(df)

# COMMAND ----------

partition_cols = ""
catalog=CATALOG
schema=SCHEMA
table_name=TABLE_APPLICATION.format(scope=scope)

df.write.format("delta").saveAsTable(f"{catalog}.{schema}.{table_name}")
