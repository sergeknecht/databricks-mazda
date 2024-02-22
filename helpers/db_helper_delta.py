# from helpers.dbx_init import spark
from databricks.sdk.runtime import *


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

def drop_table(fqn: str):
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")

def get_or_create_schema(catalog: str, schema_name: str):
    if not schema_exists(catalog, schema_name):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
    return f"{catalog}.{schema_name}"


def create_or_append_table(catalog: str, schema: str, table_name: str, df, partition_cols: list = None, overwrite: bool = False):
    mode = "overwrite"
    if table_exists(catalog, schema, table_name):
        if overwrite:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")
            mode = "overwrite"
        else:
            mode = "append"
            # raise ValueError(f"Table {catalog}.{schema}.{table_name} already exists")

    if partition_cols:
        partition_cols = ",".join(partition_cols)
        df.write.format("delta").mode(mode).partitionBy(partition_cols).saveAsTable(f"{catalog}.{schema}.{table_name}")
    else:
        partition_cols = ""
        df.write.format("delta").mode(mode).saveAsTable(f"{catalog}.{schema}.{table_name}")

    # df.write.format("delta").partitionBy(partition_cols).saveAsTable(f"{catalog}.{schema}.{table_name}")

    if overwrite:
        return f"REPLACED: {catalog}.{schema}.{table_name}"
    else:
        return f"CREATED_IF_NEW: {catalog}.{schema}.{table_name}"
