# Databricks notebook source
import json
import pprint as pp
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("catalog", "DWH_BI1")
dbutils.widgets.text("schema_filter", "like 'LZ_%'")
dbutils.widgets.dropdown("scope", "ACC", ["ACC", "PRD", "DEV"])

schema_filter = dbutils.widgets.get("schema_filter")
scope = dbutils.widgets.get("scope")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# testing environment and available login credentials
username = dbutils.secrets.get(scope="ACC", key="DWH_BI1__JDBC_USERNAME")
password = dbutils.secrets.get(scope="ACC", key="DWH_BI1__JDBC_PASSWORD")
assert dbutils.secrets.get(
    scope="ACC", key="DWH_BI1__JDBC_USERNAME"
), "secret username not retrieved"
assert dbutils.secrets.get(
    scope="ACC", key="DWH_BI1__JDBC_PASSWORD"
), "secret password not retrieved"


hostName = "accdw-scan.mle.mazdaeur.com"
# hostName="10.230.2.32"
port = "1521"
databaseName = f"{scope}_DWH"
jdbcUrl = f"jdbc:oracle:thin:@//{hostName}:{port}/{databaseName}"
catalog_name = f"{scope}__{catalog}"

db_config = {
    "db_type": "jdbc",
    "driver": "oracle.jdbc.driver.OracleDriver",
    "username": username,
    "password": password,
    "hostName": "accdw-scan.mle.mazdaeur.com",
    "port": "1521",
    "databaseName": databaseName,
    "scope": scope,
    "url": jdbcUrl,
}

pp.pprint(db_config, sort_dicts=True)

# COMMAND ----------

def get_df_sql(db_config, sql: str):
    if type(db_config) == str:
        db_config = json.loads(db_config)

    df_sql = (
        spark.read.format("jdbc")
        .option("driver", db_config["driver"])
        .option("url", db_config["url"])
        .option("query", sql)
        .option("user", db_config["username"])
        .option("password", db_config["password"])
        .load()
    )
    return df_sql


def get_df_table(db_config, table_name):
    if type(db_config) == str:
        db_config = json.loads(db_config)
    df_sql = (
        spark.read.format("jdbc")
        .option("driver", db_config["driver"])
        .option("url", db_config["url"])
        .option("dbtable", table_name)
        .option("user", db_config["username"])
        .option("password", db_config["password"])
        .load()
    )

    return df_sql


# def unity_schema_exists(catalog: str, schema_name: str):
#     query = spark.sql(
#         f"""
#             SELECT 1 
#             FROM {catalog}.information_schema.schemata 
#             WHERE schema_name = '{schema_name}' 
#             LIMIT 1"""
#     )

#     return query.count() > 0


# def unity_table_exists(catalog: str, schema: str, table_name: str):
#     query = spark.sql(
#         f"""
#             SELECT 1 
#             FROM {catalog}.information_schema.tables 
#             WHERE table_name = '{table_name}' 
#             AND table_schema='{schema}' LIMIT 1""",
#     )
#     return query.count() > 0


sql = f"""SELECT 
OWNER SCHEMA_NAME, TABLE_NAME, cast(NUM_ROWS as INT) NUM_ROWS, cast(AVG_ROW_LEN as INT) AVG_ROW_LEN, (NUM_ROWS * AVG_ROW_LEN)/1024/1024/1024 EST_SIZE_IN_GB
FROM all_tables 
WHERE owner {schema_filter}
AND TABLE_NAME not like 'SNAP_%' and  TABLE_NAME not like '%$%' 
AND NUM_ROWS > 0
ORDER BY NUM_ROWS ASC"""

df_list = get_df_sql(db_config, sql)
display(df_list)

# COMMAND ----------

task_params = [
    (
        json.dumps(db_config),
        f'{catalog_name}__{row["SCHEMA_NAME"]}__{row["TABLE_NAME"]}',
        catalog_name,
        row["SCHEMA_NAME"],
        row["TABLE_NAME"],
        scope,
    )
    for row in df_list.collect()
]
schema_str = "db_config: string, identifier: string, catalog_name: string, schema: string, table_name: string, scope: string"
udf_str = "db_config string, identifier string, catalog_name string, schema string, table_name string, scope string"
df_tasks = spark.createDataFrame(task_params, schema_str)
display(df_tasks)

# COMMAND ----------

count_max = df_tasks.rdd.count()
print(count_max)

# COMMAND ----------

from helpers.unity_helper import unity_table_exists, get_fqn

# task executed in a worker thread
# signature return type
schema_result = StructType().add("identifier", StringType()).add("result", StringType())


@udf(returnType=StringType())
def do_task(db_config, identifier, catalog_name, schema, table_name, scope):
    # block for a moment
    # Construct the fully-qualified table name
    dbx_qualified_table_name = f"{catalog_name}.{schema}.{table_name}"
    qualified_table_name = f"{schema}.{table_name}"

    func_result = unity_table_exists(catalog, schema, table_name)

    # return the resy
    return func_result


res = df_tasks.withColumn(
    "result",
    do_task(
        col("db_config"),
        col("identifier"),
        col("catalog_name"),
        col("schema"),
        col("table_name"),
        col("scope"),
    ),
)
display(res)
