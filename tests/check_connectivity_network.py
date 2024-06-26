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

# MAGIC %sh nslookup prddw-scan.mle.mazdaeur.com

# COMMAND ----------

# MAGIC %sh nslookup -type=any prddw-scan.mle.mazdaeur.com

# COMMAND ----------

# MAGIC %sh nc -zv prddw-scan.mle.mazdaeur.com 1521

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test connectivity DBX to Mazda bitbucket server https://apvlcis01.mme.mazdaeur.com/bitbucket/dashboard

# COMMAND ----------

# MAGIC %sh nc -zv apvlcis01.mme.mazdaeur.com 80

# COMMAND ----------

# MAGIC %sh nc -zv apvlcis01.mme.mazdaeur.com 443
