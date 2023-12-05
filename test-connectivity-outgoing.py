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
# MAGIC ## Python ODBC connectivity

# COMMAND ----------

# %sh
# pip install --upgrade -r requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secret Scopes Initialization from developers laptop
# MAGIC
# MAGIC This procedure requires environment variables to be set-up:
# MAGIC
# MAGIC ```dos
# MAGIC set DATABRICKS_HOST=https://mazdaeur-mazdaeur-mazda-bi20-nonprdvpc.cloud.databricks.com
# MAGIC set DATABRICKS_ACCOUNT_ID=
# MAGIC set DATABRICKS_TOKEN=
# MAGIC set DBX_TOKEN_ENDPOINT=https://accounts.cloud.databricks.com/oidc/accounts/%DATABRICKS_ACCOUNT_ID%/v1/token
# MAGIC set DBT_ORACLE_USER=
# MAGIC set DBT_ORACLE_PASSWORD=
# MAGIC
# MAGIC databricks configure --host https://mazdaeur-mazdaeur-mazda-bi20-nonprdvpc.cloud.databricks.com --profile DEFAULT
# MAGIC databricks secrets create-scope jdbc
# MAGIC ```
# MAGIC
# MAGIC and secrets to have been saved to scope:
# MAGIC
# MAGIC ```dos
# MAGIC databricks secrets put-secret jdbc username --string-value %DBT_ORACLE_USER%
# MAGIC databricks secrets put-secret jdbc password --string-value %DBT_ORACLE_PASSWORD%
# MAGIC ```

# COMMAND ----------

username = dbutils.secrets.get(scope="ACC", key="DWH_BI1__JDBC_USERNAME")
password = dbutils.secrets.get(scope="ACC", key="DWH_BI1__JDBC_PASSWORD")

assert username, "secret username not retrieved"
assert password, "secret password not retrieved"

# for char in username:
#     print(char, end=" ")

# COMMAND ----------

hostName = "accdw-scan.mle.mazdaeur.com"
# hostName="10.230.2.32"
port = "1521"
databaseName = "ACC_DWH"

jdbcUrl = f"jdbc:oracle:thin:@//{hostName}:{port}/{databaseName}"
print(jdbcUrl)

# COMMAND ----------

employees_table = (
    spark.read.format("jdbc")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "LZ_MUM.TUSER")
    .option("user", username)
    .option("password", password)
    .load()
)

# COMMAND ----------

display(employees_table)

# COMMAND ----------

# MAGIC %md
# MAGIC **FOLLOWING DOES NOT WORK WITH SECRETS**
# MAGIC
# MAGIC Following requires secrets to be loaded via spark ini into environment using
# MAGIC
# MAGIC `spark.password {{secrets/scope1/key1}}`
# MAGIC
# MAGIC source: <https://docs.databricks.com/en/security/secrets/secrets.html#reference-a-secret-with-a-spark-configuration-property>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT secret('ACC', 'DWH_BI1__JDBC_DRIVER');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW vw_employees_table
# MAGIC USING JDBC
# MAGIC OPTIONS (
# MAGIC   url "jdbc:oracle:thin:@//10.230.2.32:1521/ACC_DWH",
# MAGIC   dbtable "LZ_MUM.TUSER",
# MAGIC   user secret('ACC', 'DWH_BI1__JDBC_USERNAME'),
# MAGIC   password secret('ACC', 'DWH_BI1__JDBC_PASSWORD'),
# MAGIC   driver 'oracle.jdbc.driver.OracleDriver'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_employees_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vw_employees_table;

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO**: download all views (data) from:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT VIEW_NAME, OWNER FROM SYS.ALL_VIEWS
# MAGIC WHERE OWNER = 'DWPBI'
# MAGIC ORDER BY OWNER, VIEW_NAME
# MAGIC ```
