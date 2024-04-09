# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import time

import pyspark.sql.functions as F
from pyspark.sql.functions import col

from helpers.app_helper import init
from helpers.logger_helper import log_to_delta
from helpers.status_helper import create_status

# COMMAND ----------

dbutils.widgets.dropdown(
    "jp_scope",
    "PRD",
    ["DEV", "DEV2", "ACC", "PRD", "TST"],
    label="UC catalog prefix (=scope)",
)

# COMMAND ----------

jp_scope: str = dbutils.widgets.get("jp_scope")

# COMMAND ----------

directory_path = "dbfs:/mnt/bronze/cxu/out/raw/"
dir_path_processed = "dbfs:/mnt/bronze/cxu/processed/"
file_retention_days = 7
ux_current_ts = (spark.sql("select unix_timestamp(current_timestamp())").collect()[0][0])
ux_start_ts = (ux_current_ts - (3600*24 * file_retention_days)) * 1000
ux_start_ts
catalog = "hive_metastore"
schema = "mle_bi_lz"

# COMMAND ----------

init(scope=jp_scope, catalog=catalog, schema=schema)

# COMMAND ----------

# DBTITLE 1,Assure that directory where processed and old files are moved to exists
dbutils.fs.mkdirs(dir_path_processed)

# COMMAND ----------

# DBTITLE 1,Check if mounts are available
display(dbutils.fs.ls("dbfs:/mnt"))

# COMMAND ----------

# DBTITLE 1,Get ALL files
files = dbutils.fs.ls(directory_path)
if len(files) == 0:
  # no new files available, exiting
  dbutils.notebook.exit("{}")

# COMMAND ----------

display(files)

# COMMAND ----------

# DBTITLE 1,Filter out files that follow the agreed naming convention <data type>__date.csv.gz
df = spark.createDataFrame([file for file in files if "__" in file.path and file.path.endswith('.csv.gz')])

if df.count() == 0:
  # no new files available, exiting
  dbutils.notebook.exit("{}")

get_file_name = lambda x: x.rsplit('/', 1)[-1]
get_data_type = lambda x: x.split("__")[0]
df = df.withColumn("file_name", F.udf(lambda x: get_file_name(x))("path"))
df = df.withColumn("data_type", F.udf(lambda x: get_data_type(x))("file_name"))
df = df.withColumn("file_dt",  F.from_unixtime(col("modificationTime") / 1000)) # F.udf(lambda x: get_dt(x))("modificationTime"))
df = df.alias("df")
df.show()

# COMMAND ----------

# DBTITLE 1,Split the files in 1 categories, LAST (to be processed) and NOT LAST (to be moved to processed archive)
df_lf = df.groupby('data_type').agg(F.max('modificationTime').alias("modificationTime")).alias("df_lf")
# df_lf = df_lf.filter(F.col('modificationTime') > 1709901250000)
df_not_process = df.join(df_lf,
               (df.data_type == df_lf.data_type) & (df.modificationTime == df_lf.modificationTime),
               "left_anti").select("df.*")
df_todo = df.join(df_lf,
               (df.data_type == df_lf.data_type) & (df.modificationTime == df_lf.modificationTime),
               "inner").select("df.*")

# COMMAND ----------

def move_file_to_processed(row):
  path = row["path"]
  file_name_archive = os.path.join(dir_path_processed, row["name"])
  dbutils.fs.mv(path, file_name_archive)
  print("\t --> " + file_name_archive)

# COMMAND ----------

# DBTITLE 1,Move NON-LAST files immediately to archive

if df_not_process.count() > 0:
  print("NON-LAST files detected. Moving them to processed dir.")
  df_not_process.show()
  for row in df_not_process.collect():
    move_file_to_processed(row)
else:
  print("no NON-LAST files to be moved to archive.")

# COMMAND ----------

# DBTITLE 1,Check if we have LAST files to be processed, if not quit
if df_todo.count() == 0:
  # no new files available, exiting
  dbutils.notebook.exit("{}")

# COMMAND ----------

display(df_todo)

# COMMAND ----------

# DBTITLE 1,Import each LAST file into delta table + log entry + move ot to processed dir
for row in df_todo.collect():
    start_time = time.time()
    data_type_db = row['data_type'].lower()
    file_dt = row["file_dt"]
    file_name = row["file_name"]
    print(row['data_type'])
    fqn = f"hive_metastore.mle_bi_lz.{row['data_type'].lower()}"
    df_data = spark.read.csv(row['path'], sep=',', header=True, inferSchema=True)
    df_data = df_data.withColumn("file_dt", F.lit(file_dt))
    df_data = df_data.withColumn("file_name", F.lit(file_name))
    df_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
    end_time = time.time()
    time_duration = int(end_time - start_time)
    result = create_status(
                scope=jp_scope,
                status_code=201,
                status_message=f"imported {file_name}",
            )
    result["row_count"] = df_data.count()
    result["fqn"] = fqn
    result["time_duration"] = time_duration
    log_to_delta(result, catalog=catalog, schema=schema)

    move_file_to_processed(row)

# COMMAND ----------

# MAGIC %md
# MAGIC - dateFormat Default value: yyyy-MM-dd
# MAGIC - encoding Default value: UTF-8
# MAGIC - sep or delimiter Default value: ","
# MAGIC - timeZone Default value: None The java.time.ZoneId to use when parsing timestamps and dates.
# MAGIC - escape Default value: '\'
# MAGIC - Add timestamp date of import
# MAGIC - replace table landing zone

# COMMAND ----------

# DBTITLE 1,Get all files from archive dir
files = dbutils.fs.ls(dir_path_processed)
df = spark.createDataFrame([file for file in files if "__" in file.path and file.path.endswith('.csv.gz')])
display(df)

# COMMAND ----------

# DBTITLE 1,Clean-up old files (using retention period days parameter)
# ux_start_ts = 1709896854000
df_files = spark.createDataFrame(files).withColumn("file_dt",  F.from_unixtime(col("modificationTime") / 1000))
df_files_old = df_files.filter(F.col('modificationTime') < ux_start_ts)
df_files_old.show()
for row in df_files_old.collect():
    file_name = row["path"]
    print(f"Deleting: {file_name}")
    dbutils.fs.rm(file_name)

# COMMAND ----------

# DBTITLE 1,Procedure finished. Quiting nicely ...
dbutils.notebook.exit("{}")

# COMMAND ----------

# MAGIC %md
# MAGIC [AWS DBX auto-loader patterns](https://docs.databricks.com/en/ingestion/auto-loader/patterns.html)

# COMMAND ----------

data_type = "MME_DS_Customer_Table_SubsetFields"
data_type_db = "raw_" + data_type.lower()
table_name = f"hive_metastore.mle_bi_lz.{data_type_db}"
source_format = "csv"
header = "true"

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", source_format)
    .option("header", header)
    .option("rescuedDataColumn", "_rescued_data")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"/mnt/bronze/cxu/mle_bi_lz/schema/{data_type_db}")
    .option("mergeSchema", "true")
    .option("modifiedAfter", "2022-10-15 11:34:00.000000 UTC-3")  # To ingest files that have a modification timestamp after the provided timestamp.
    .load("/mnt/bronze/cxu/out/raw/{data_type}__*.csv.gz")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"/mnt/bronze/cxu/mle_bi_lz/checkpoint/{data_type_db}")
    .trigger(availableNow=True)
    .toTable(table_name)
)
df
