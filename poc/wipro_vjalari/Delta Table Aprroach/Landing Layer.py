# Databricks notebook source
# MAGIC %sql
# MAGIC Truncate table bronze.lztbcoc_product

# COMMAND ----------


tbcoc_product = (
    spark.read.format('csv')
    .option('inferSchema', 'true')
    .option('header', 'true')
    .load('s3://mazda-bi20-data-nonprd/data/Bronze/tbcoc_product.csv')
)
# display(tbcoc_product)
tbcoc_product.write.format('delta').mode('overwrite').saveAsTable(
    'bronze.lztbcoc_product'
)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC Truncate table bronze.lztbcoc_value

# COMMAND ----------

LZTBCOC_VALUE = (
    spark.read.format('csv')
    .option('inforeSchema', 'true')
    .option('header', 'true')
    .load('s3://mazda-bi20-data-nonprd/data/Bronze/LZTBCOC_VALUE.csv')
)

LZTBCOC_VALUE_modified = (
    LZTBCOC_VALUE.withColumn('DISTRIBUTOR', col('DISTRIBUTOR').cast(IntegerType()))
    .withColumn('LANGUAGE', col('LANGUAGE').cast(IntegerType()))
    .withColumn('COCNUMBER', col('COCNUMBER').cast(IntegerType()))
    .withColumn('VALUEID', col('VALUEID').cast(IntegerType()))
    .withColumn('STATUS', col('STATUS').cast(IntegerType()))
)
LZTBCOC_VALUE_modified.write.format('delta').mode('overwrite').saveAsTable(
    'bronze.LZTBCOC_VALUE'
)


# COMMAND ----------

# MAGIC %sql
# MAGIC Truncate table bronze.LZBTNCOCV

# COMMAND ----------

path = 's3://mazda-bi20-data-nonprd/data/Bronze/LZBTNCOCV.csv'

LZBTNCOCV = (
    spark.read.format('csv')
    .option('inforeSchema', 'true')
    .option('header', 'true')
    .load(path)
)

LZBTNCOCV_modified = LZBTNCOCV.withColumn('SEND_NBR', col('SEND_NBR').cast(IntegerType()))
# .withColumn("COC_DT",to_date(col("COC_DT"),'dd-MMM-yy'))
LZBTNCOCV_modified.write.format('delta').mode('overwrite').saveAsTable('bronze.LZBTNCOCV')
