# Databricks notebook source
display(dbutils.fs.ls("s3://mazda-bi20-data-nonprd/data/Bronze/"))


# COMMAND ----------

# MAGIC %sql
# MAGIC create database LANDING_ZONE_COC

# COMMAND ----------

# MAGIC %sql
# MAGIC create database main.LANDING_ZONE_COC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table landing_zone_coc.tbcoc_product

# COMMAND ----------

df1 = spark.read.format('csv').option('inferSchema','true').option('header','true').load("s3://mazda-bi20-data-nonprd/data/Bronze/tbcoc_product.csv")
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database COC

# COMMAND ----------

path = "s3://mazda-bi20-data-nonprd/data/Bronze/LZBTNCOCV.csv"

BTNCOCV = spark.read.format("csv").option("inforeSchema", "true").option("header", "true").load(path)

#BTNCOCV.write.format("delta").mode("overwrite").saveAsTable("COC.BTNCOCV")

display(BTNCOCV)

# COMMAND ----------


LZTBCOC_VALUE = spark.read.format("csv").option("inforeSchema", "true").option("header", "true").load("s3://mazda-bi20-data-nonprd/data/Bronze/LZTBCOC_VALUE.csv")

#LZTBCOC_VALUE.write.format("delta").mode("inferSchema","true").mode("overwrite").saveAsTable("COC.LZTBCOC_VALUE")

display(LZTBCOC_VALUE)

# COMMAND ----------

LZTBCOC_VALUE = spark.read.format("csv").option("inforeSchema", "true").option("header", "true").load("s3://mazda-bi20-data-nonprd/data/Bronze/LZTBCOC_VALUE.csv")

#LZTBCOC_VALUE.write.format("delta").mode("inferSchema","true").mode("overwrite").saveAsTable("COC.LZTBCOC_VALUE")

display(LZTBCOC_VALUE)

# COMMAND ----------

df1 = spark.read.format('csv').option('inferSchema','true').option('header','true').load("s3://mazda-bi20-data-nonprd/data/Bronze/tbcoc_product.csv")
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.dim_vin limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from default.f
