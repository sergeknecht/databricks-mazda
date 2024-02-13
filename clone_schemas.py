# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import sys
import json
import traceback
from helpers.db_helper import (
    get_db_dict,
    get_bounds__by_rownum,
    get_connection_properties__by_key,
    get_data,
    get_data_partitioned__by_rownum,
    schema_exists,
    table_exists,
)
from helpers.db_helper_oracle_sql import sql_pk_statement
from helpers.status_helper import create_status
import time

# COMMAND ----------

# MAGIC %md # Impetus Clone

# COMMAND ----------

start_time = time.time()
catalog_source = "impetus_ref"
catalog_target = "impetus_poc"
schemas = ["stg"]

def get_tables(catalog: str, schema: str):
    query = spark.sql(
        f"""
            SHOW TABLES IN {catalog}.{schema}"""
    )

    return query.collect()

def schema_exists(catalog: str, schema_name: str):
    query = spark.sql(
        f"""
            SELECT 1 
            FROM {catalog}.information_schema.schemata 
            WHERE schema_name = '{schema_name}' 
            LIMIT 1"""
    )

    return query.count() > 0

def table_exists(catalog: str, schema: str, table_name: str):
    query = spark.sql(
        f"""
            SELECT 1 
            FROM {catalog}.information_schema.tables 
            WHERE table_name = '{table_name}' 
            AND table_schema='{schema}' LIMIT 1""",
    )
    return query.count() > 0

def run_sql(sql: str):
    return spark.sql(sql).collect()

# COMMAND ----------

tables = []
for schema in schemas:
    if schema_exists(catalog_source, schema):
        df = get_tables(catalog_source, "stg")
        df = [{"table": row["tableName"], "schema": row["database"]} for row in df]
        df = [row for row in df if not table_exists(catalog_target, row["schema"], row["table"])]


display(df)

# COMMAND ----------

schema = ""

sql = f"USE CATALOG {catalog_source}"
run_sql(sql)

for row in df:
    schema_new = row["schema"]
    if schema != schema_new:
        schema = schema_new
        sql = f"USE {schema}"
        run_sql(sql)

        sql = f"CREATE SCHEMA IF NOT EXISTS {catalog_target}.{schema};"
        run_sql(sql)
    
    table = row["table"]
    if not table_exists(catalog_target, schema, table):
        sql = f"""
                CREATE TABLE IF NOT EXISTS {catalog_target}.{schema}.{table}
                    DEEP CLONE {catalog_source}.{schema}.{table};
        """
        print(sql)
        run_sql(sql)

# COMMAND ----------

dbutils.notebook.exit(create_status("OK: Notebook Completed"))
