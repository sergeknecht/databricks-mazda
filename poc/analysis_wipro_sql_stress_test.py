# Databricks notebook source
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
hostname = context.apiUrl().get()
token = context.apiToken().get()

# COMMAND ----------

header={'Authorization':f'Bearer {token}','Content-Type':'application/json'}
url = f"{hostname}/api/2.0/sql/history/queries"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT  unix_millis(to_timestamp("2023-09-26 12:39:00Z")) as filter_start_time, unix_millis(to_timestamp("2023-09-26 12:51:00Z")) as filter_end_time
# MAGIC SELECT  unix_millis(to_timestamp("2023-10-20 12:41:00Z")) as filter_start_time, unix_millis(to_timestamp("2023-09-26 12:51:00Z")) as filter_end_time

# COMMAND ----------

start_time_ms=1695731940000
end_time_ms=1695732660000 # 1695732060000
status= "FINISHED"
max_results=1000

# COMMAND ----------

payload = {
"filter_by": {
    "query_start_time_range": {
      "end_time_ms": end_time_ms,
      "start_time_ms": start_time_ms
    },
    "statuses": status, # ["QUEUED","RUNNING","CANCELED","FAILED","FINISHED"]
    "user_ids": [], # array of integers
    "warehouse_ids" : [] # array of strings
  },
  "include_metrics": True,
  "max_results": max_results # Number of results for one page. The default is 100.
}

# COMMAND ----------

import json
import requests
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# json to dataframe
def to_df(result_json):
  return (
    spark.createDataFrame(result_json)
    .withColumn("execution_end_time", 
         F.lit(F.col("execution_end_time_ms")/1000).cast("timestamp"))
   )

data = json.dumps(payload)
lst_df = []
has_next_page = True
while has_next_page:
  response = requests.get(url=url, data=data, headers=header)
  if 'res' in response.json():
    lst_df.append(to_df(response.json()['res']))

  has_next_page = response.json()['has_next_page']
  if has_next_page:
    next_page_token = response.json()['next_page_token']
    payload = {
      "include_metrics": True,
      "max_results": max_results,
      "page_token": next_page_token
    }
    data = json.dumps(payload)

# Union all the dataframe into 
if lst_df:
  final_df = reduce(DataFrame.unionAll, lst_df)
  final_df.createOrReplaceTempView("vwQueryHistory")
  final_df.createOrReplaceTempView("vwQueryHistory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## to add later 
# MAGIC
# MAGIC - but with delta
# MAGIC - to have ability to query from SQL only notebooks/queries
# MAGIC
# MAGIC ```python
# MAGIC df.write \
# MAGIC   .format("parquet") \
# MAGIC   .option("compression", "snappy") \
# MAGIC   .mode("overwrite") \
# MAGIC   .partitionBy("year", "month", "day") \
# MAGIC   .saveAsTable("database_name.table_name")
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(timestamp_millis(query_start_time_ms), "HHmmss") as query_start_time,  (duration / 1000) as duration_sec, 
# MAGIC  --   count(1) as count_queries,
# MAGIC  --   count(distinct executed_as_user_id) as count_users,
# MAGIC     * 
# MAGIC FROM QueryHistory
# MAGIC ORDER BY duration DESC

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED `hive_metastore`.`default`.`dim_cal`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail `hive_metastore`.`default`.`dim_cal`

# COMMAND ----------


