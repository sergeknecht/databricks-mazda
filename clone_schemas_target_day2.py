# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json
import sys
import time
import traceback

from helpers.db_helper_delta import schema_exists, table_exists
from helpers.db_helper_jdbc import (
    get_connection_properties__by_key,
    get_data,
    get_db_dict,
    get_jdbc_bounds__by_rownum,
)
from helpers.db_helper_jdbc_oracle import get_data_partitioned__by_rownum
from helpers.db_helper_sql_oracle import sql_pk_statement
from helpers.status_helper import create_status

# COMMAND ----------

# MAGIC %md # Impetus Clone

# COMMAND ----------

start_time = time.time()
catalog_source = 'acc__impetus_target'
catalog_target = 'impetus_target_day2'
schemas = ['stg', 'stg_tmp', 'lz_lem']
jp_action = "drop__create"
jp_actions = jp_action.split('__')

def get_tables(catalog: str, schema: str):
    return spark.sql(
        f"""
            SHOW TABLES IN {catalog}.{schema}"""
    ).collect()

def run_sql(sql: str):
    return spark.sql(sql).collect()


# COMMAND ----------

sql = f'USE CATALOG {catalog_source}'
run_sql(sql)

# COMMAND ----------

results = get_tables(catalog_source, "stg")
results = [
                {'table': row['tableName'], 'schema': row['database']} for row in results
            ]
results
table_exists(catalog_target, results[0]['schema'], results[0]['table'])


# COMMAND ----------

tables = []
for schema in schemas:
    if schema_exists(catalog_source, schema):
        results = get_tables(catalog_source, schema)
        if len(results) > 0:
            results = [
                {'table': row['tableName'], 'schema': row['database']} for row in results
            ]
            results = [
                row
                for row in results
                # if 'drop' not in jp_actions and not table_exists(catalog_target, row['schema'], row['table'])
            ]
            tables.extend(results)


display(tables)

# COMMAND ----------

schema = ''

sql = f'USE CATALOG {catalog_source}'
run_sql(sql)

for row in tables:
    schema_new = row['schema']
    table = row['table']
    if schema != schema_new:
        schema = schema_new
        sql = f'USE {schema}'
        run_sql(sql)

    sql = f'CREATE SCHEMA IF NOT EXISTS {catalog_target}.{schema}'
    run_sql(sql)

    if 'drop' in jp_actions:
        sql = f'DROP TABLE IF EXISTS {catalog_target}.{schema}.{table}'
        print(sql)
        run_sql(sql)

    if not table_exists(catalog_target, schema, table):
        sql = f"""CREATE TABLE IF NOT EXISTS {catalog_target}.{schema}.{table}
                    DEEP CLONE {catalog_source}.{schema}.{table}"""
        print(sql)
        run_sql(sql)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(create_status('OK: Notebook Completed')))
