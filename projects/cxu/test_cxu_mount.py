# Databricks notebook source
display(dbutils.fs.ls("/mnt"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze/cxu/out/raw/"))

# COMMAND ----------

df = spark.read.csv("/mnt/bronze/cxu/out/raw/*.csv.gz", sep=',', header=True, inferSchema=True)
display(df)

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("header", "true") \
  .option("rescuedDataColumn", "_rescued_data") \ # makes sure that you don't lose data
  .option("cloudFiles.inferColumnTypes", "true") \
##  .schema(<schema>) \ # alternative scenario: provide a schema here for the files
  .load("/mnt/bronze/cxu/out/raw/*.csv.gz")
df
