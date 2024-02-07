# Databricks notebook source
# MAGIC %md
# MAGIC # Test Network connectivity
# MAGIC
# MAGIC This notebook will perform various network connectivity options

# COMMAND ----------

# MAGIC %sh ifconfig | grep inet

# COMMAND ----------

# MAGIC %md
# MAGIC HTTPS out - to pypi library

# COMMAND ----------

# MAGIC %sh nc -zv pypi.org 443

# COMMAND ----------

# MAGIC %md
# MAGIC ## Database host and port - ACP

# COMMAND ----------

# MAGIC %sh nc -zv 10.230.2.32 1521

# COMMAND ----------

# MAGIC %sh cat /etc/resolv.conf

# COMMAND ----------

# MAGIC %sh nslookup accdw-scan.mle.mazdaeur.com

# COMMAND ----------

# MAGIC %sh nslookup -type=any accdw-scan.mle.mazdaeur.com

# COMMAND ----------

# MAGIC %sh nc -zv accdw-scan.mle.mazdaeur.com 1521

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connectivity to AWS Frankfurt - TCMR3

# COMMAND ----------

# MAGIC %sh nc -zv 10.229.71.68 3306

# COMMAND ----------

# MAGIC %sh nslookup tcmr-mysqldb.cndpqrinc7hg.eu-central-1.rds.amazonaws.com

# COMMAND ----------

# MAGIC %sh nc -zv tcmr-mysqldb.cndpqrinc7hg.eu-central-1.rds.amazonaws.com 3306
