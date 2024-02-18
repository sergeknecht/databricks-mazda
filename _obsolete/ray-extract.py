# Databricks notebook source
import json
import pprint as pp

from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('catalog', 'DWH_BI1')
dbutils.widgets.text('schema_filter', "like 'LZ_%'")
dbutils.widgets.dropdown('scope', 'ACC', ['ACC', 'PRD', 'DEV'])

schema_filter = dbutils.widgets.get('schema_filter')
scope = dbutils.widgets.get('scope')
catalog = dbutils.widgets.get('catalog')

# COMMAND ----------

# testing environment and available login credentials
username = dbutils.secrets.get(scope='ACC', key='DWH_BI1__JDBC_USERNAME')
password = dbutils.secrets.get(scope='ACC', key='DWH_BI1__JDBC_PASSWORD')
assert dbutils.secrets.get(
    scope='ACC', key='DWH_BI1__JDBC_USERNAME'
), 'secret username not retrieved'
assert dbutils.secrets.get(
    scope='ACC', key='DWH_BI1__JDBC_PASSWORD'
), 'secret password not retrieved'


hostName = 'accdw-scan.mle.mazdaeur.com'
# hostName="10.230.2.32"
port = '1521'
databaseName = f'{scope}_DWH'
jdbcUrl = f'jdbc:oracle:thin:@//{hostName}:{port}/{databaseName}'
catalog_name = f'{scope}__{catalog}'

db_config = {
    'db_type': 'jdbc',
    'driver': 'oracle.jdbc.driver.OracleDriver',
    'username': username,
    'password': password,
    'hostName': 'accdw-scan.mle.mazdaeur.com',
    'port': '1521',
    'databaseName': databaseName,
    'scope': scope,
    'url': jdbcUrl,
}

pp.pprint(db_config, sort_dicts=True)

# COMMAND ----------


def get_df_sql(db_config, sql: str):
    if type(db_config) == str:
        db_config = json.loads(db_config)

    df_sql = (
        spark.read.format('jdbc')
        .option('driver', db_config['driver'])
        .option('url', db_config['url'])
        .option('query', sql)
        .option('user', db_config['username'])
        .option('password', db_config['password'])
        .load()
    )
    return df_sql


def get_df_table(db_config, table_name):
    if type(db_config) == str:
        db_config = json.loads(db_config)
    df_sql = (
        spark.read.format('jdbc')
        .option('driver', db_config['driver'])
        .option('url', db_config['url'])
        .option('dbtable', table_name)
        .option('user', db_config['username'])
        .option('password', db_config['password'])
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
        row['SCHEMA_NAME'],
        row['TABLE_NAME'],
        scope,
    )
    for row in df_list.collect()
]
schema_str = 'db_config: string, identifier: string, catalog_name: string, schema: string, table_name: string, scope: string'
udf_str = 'db_config string, identifier string, catalog_name string, schema string, table_name string, scope string'
df_tasks = spark.createDataFrame(task_params, schema_str)
display(df_tasks)

# COMMAND ----------

task_params = [
    {  #
        'db_config': row['db_config'],
        'id': row['identifier'],
        'catalog': row['catalog_name'],
        'schema': row['schema'],
        'table': row['table_name'],
        'scope': row['scope'],
    }
    for row in df_task_collected
]
display(task_params[:3])

# COMMAND ----------

# MAGIC %pip install ray[default]==2.3.0 >/dev/null

# COMMAND ----------

from ray.util.spark import MAX_NUM_WORKER_NODES, setup_ray_cluster, shutdown_ray_cluster

# COMMAND ----------

setup_ray_cluster(
    num_worker_nodes=2,
    num_cpus_per_node=4,
    collect_log_to_path='/dbfs/tmp/raylogs',
)

# COMMAND ----------

import ray

ray.init()
ray.cluster_resources()

# COMMAND ----------

from helpers.unity_helper import get_fqn, unity_table_exists


@ray.remote
def do_task(db_config, identifier, catalog_name, schema_name, table_name, scope):
    # block for a moment
    # Construct the fully-qualified table name
    dbx_qualified_table_name = f'{catalog_name}.{schema_name}.{table_name}'
    qualified_table_name = f'{schema_name}.{table_name}'

    table_exists = unity_table_exists(catalog_name, schema_name, table_name)

    return {
        'table_fqn': dbx_qualified_table_name,
        'table_exists': table_exists,
        'table_qn': qualified_table_name,
    }


results = [
    do_task.remote(
        row['db_config'],
        row['id'],
        row['catalog'],
        row['schema'],
        row['table'],
        row['scope'],
    )
    for row in task_params
]

output = ray.get(results)

display(output)

# COMMAND ----------

shutdown_ray_cluster()
