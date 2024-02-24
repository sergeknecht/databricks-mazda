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
# MAGIC databricks secrets create-scope ACC
# MAGIC ```
# MAGIC
# MAGIC and secrets to have been saved to scope:
# MAGIC
# MAGIC ```dos
# MAGIC databricks secrets put-secret ACC username --string-value %DBT_ORACLE_USER%
# MAGIC databricks secrets put-secret ACC password --string-value %DBT_ORACLE_PASSWORD%
# MAGIC ```

# COMMAND ----------

from helpers.db_helper_jdbc import (
    get_bounds__by_rownum,
    get_connection_properties__by_key,
    get_jdbc_data_by_dict,
)
from helpers.db_helper_jdbc_oracle import get_data_partitioned__by_rownum

# COMMAND ----------

SCOPE = 'ACC' or 'PRD' or 'ACC'
DB_KEY = 'DWH_BI1'


# COMMAND ----------

db_conn_props = get_connection_properties__by_key(SCOPE, DB_KEY)

print(db_conn_props['url'])


# COMMAND ----------

# pushdown_query = """(
# select  1 as MIN_ID, count(*) as MAX_ID
# from LZ_MUM.TUSER
# ) dataset
# """

# alternative  min(some_uniform_dictributed_int_column) as MIN_ID, max(some_uniform_dictributed_int_column) as MAX_ID
# bounds = spark.read.jdbc(
#     url=jdbcUrl, table=pushdown_query, properties=connectionProperties
# ).collect()[0]

bounds = get_bounds__by_rownum(db_conn_props, 'LZ_MUM.TUSER')
bounds


# COMMAND ----------

# query with ROWNULber which will allow us to create partitions
# when writing the writes will be distributed by partition

df_mum = get_data_partitioned__by_rownum(
    db_conn_props, 'LZ_MUM.TUSER', bounds, order_by_column='GUID'
)

display(df_mum)


# COMMAND ----------

# query with ROWNULber which will allow us to create partitions
# when writing the writes will be distributed by partition

df_mum = get_data(db_conn_props, 'SELECT * FROM LZ_MUM.TUSER ORDER BY GUID')

display(df_mum)


# COMMAND ----------

pushdown_query = """(
select ROWNUM, d.*
from LZ_MUM.TUSER d
order by d.GUID
) dataset
"""

df_mum = spark.read.jdbc(
    url=jdbcUrl,
    table=pushdown_query,
    properties=connectionProperties,
    numPartitions=4,
    column='ROWNUM',
    lowerBound=bounds.MIN_ID,
    upperBound=bounds.MAX_ID + 1,
)

display(df_mum)

# COMMAND ----------

df_mum_parts = (
    spark.read.format('jdbc')
    # .option("url", jdbcUrl)
    .option('dbtable', pushdown_query)
    # .option("driver", driver)
    # .option("user", username)
    # .option("password", password)
    # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
    .option('partitionColumn', 'ROWNUM')
    # lowest value to pull data for with the partitionColumn
    .option('lowerBound', f'{bounds.MIN_ID:.0f}')
    # max value to pull data for with the partitionColumn
    .option('upperBound', f'{bounds.MAX_ID+1:.0f}')
    # number of partitions to distribute the data into. Do not set this very large (~hundreds)
    .option('numPartitions', 4)
    # # Oracle’s default fetchSize is 10
    # .option("fetchSize", "100")
    .options(**connectionProperties).load()
)
display(df_mum_parts)

# COMMAND ----------

df_mum_parts.rdd.getNumPartitions()

# COMMAND ----------

# single partition
df_mum_1_part = (
    spark.read.format('jdbc')
    # .option("driver", driver)
    # .option("url", jdbcUrl)
    .option('dbtable', 'LZ_MUM.TUSER')
    # .option("user", username)
    # .option("password", password)
    # .option("fetchSize", "100")
    .options(**connectionProperties).load()
)

# COMMAND ----------

display(df_mum_1_part)

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
# MAGIC SELECT
# MAGIC   secret('ACC', 'DWH_BI1__JDBC_DRIVER');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW vw_employees_table USING JDBC OPTIONS (
# MAGIC   url "jdbc:oracle:thin:@//accdw-scan.mle.mazdaeur.com:1521/ACC_DWH",
# MAGIC   dbtable "LZ_MUM.TUSER",
# MAGIC   user secret('ACC', 'DWH_BI1__JDBC_USERNAME'),
# MAGIC   password secret('ACC', 'DWH_BI1__JDBC_PASSWORD'),
# MAGIC   driver "oracle.jdbc.driver.OracleDriver",
# MAGIC   fetchSize '100'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   vw_employees_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   count(*)
# MAGIC from
# MAGIC   vw_employees_table;

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO**: download all views (data) from:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT VIEW_NAME, OWNER FROM SYS.ALL_VIEWS
# MAGIC WHERE OWNER = 'DWPBI'
# MAGIC ORDER BY OWNER, VIEW_NAME
# MAGIC ```
