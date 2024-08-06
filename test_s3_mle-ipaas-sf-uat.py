# Databricks notebook source
import os

# COMMAND ----------

s3_ipaas_in = 's3://mazda-csv-data-nonprd/mle-ipaas-sf-uat'

fs = dbutils.fs.ls(s3_ipaas_in)
assert fs, f'No files found, or bucket not accessible: {s3_ipaas_in}'
display(fs)

# COMMAND ----------

entity = "account"
sub_dir = os.path.join(s3_ipaas_in, entity, "")
fs = dbutils.fs.ls(sub_dir)
assert fs, f'No files found, or bucket not accessible: {sub_dir}'
display(fs)
