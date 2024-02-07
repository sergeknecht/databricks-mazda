# Databricks notebook source
# MAGIC %md
# MAGIC # Python ODBC connectivity
# MAGIC This notebook will test jdbc connectivity and several query scenario's

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

connectionProperties = {
  "user" : username,
  "password" : password,
  "driver" : "oracle.jdbc.driver.OracleDriver",
  "fetchSize": "100"
}

# COMMAND ----------


pushdown_query = """(
select  1 as MIN_ID, count(*) as MAX_ID
from LZ_MUM.TUSER
) dataset
"""

# alternative  min(some_uniform_dictributed_int_column) as MIN_ID, max(some_uniform_dictributed_int_column) as MAX_ID

spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties).collect()
bounds = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties).collect()[0]

bounds

# COMMAND ----------

type(bounds)

# COMMAND ----------

pushdown_query = """(
select ROWNUM, d.*
from LZ_MUM.TUSER d
order by d.GUID
) dataset
"""

# COMMAND ----------

df_mum = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, 
                             properties=connectionProperties, 
                             numPartitions=4,
                             column="ROWNUM",
                             lowerBound=bounds.MIN_ID, 
                             upperBound=bounds.MAX_ID + 1)

display(df_mum)

# COMMAND ----------

df_mum = (spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", pushdown_query)
  .option("driver", "oracle.jdbc.driver.OracleDriver")
  .option("user", username)
  .option("password", password)
  # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
  .option("partitionColumn", "ROWNUM")
  # lowest value to pull data for with the partitionColumn
  .option("lowerBound", f"{bounds.MIN_ID:.0f}")
  # max value to pull data for with the partitionColumn
  .option("upperBound", f"{bounds.MAX_ID+1:.0f}")
  # number of partitions to distribute the data into. Do not set this very large (~hundreds)
  .option("numPartitions", 4)
  # Oracle’s default fetchSize is 10
  .option("fetchSize", "100")
  .load()
)
display(df_mum)

# COMMAND ----------

df_mum.rdd.getNumPartitions()

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