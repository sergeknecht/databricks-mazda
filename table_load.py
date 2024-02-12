# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
from helpers.db_helper import (
    get_db_dict,
    get_bounds__by_rownum,
    get_connection_properties__by_key,
    get_data,
    get_data_partitioned__by_rownum,
    schema_exists,
    table_exists
)
from helpers.db_helper_oracle_sql import sql_pk_statement
from helpers.status_helper import create_status

# COMMAND ----------

dbutils.widgets.dropdown("p_scope", "ACC", choices=["ACC", "PRD"], label="Development Scope")
dbutils.widgets.text("p_catalog_name_target", "impetus_ref", label="Catalog Name Target")
dbutils.widgets.text("p_schema_name_source", "STG", label="Schema Name Source")
dbutils.widgets.text("p_table_name_source", "STG_LEM_TRANSPORT_MODES", label="Table Name Source")

# COMMAND ----------

# Run all tests in the repository root.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(notebook_path) # os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')
%pwd

# COMMAND ----------

p_scope = dbutils.widgets.get("p_scope")
p_catalog_name_target = dbutils.widgets.get("p_catalog_name_target").lower()
p_schema_name_target = dbutils.widgets.get("p_schema_name_source").lower()
p_table_name_target = dbutils.widgets.get("p_table_name_source").lower()
p_schema_name_source = dbutils.widgets.get("p_schema_name_source")
p_table_name_source = dbutils.widgets.get("p_table_name_source")


dbx_qualified_table_name = f"{p_catalog_name_target}.{p_schema_name_target}.{p_table_name_target}"
print(dbx_qualified_table_name)

# COMMAND ----------

SCOPE = "ACC" or "PRD" or "ACC"
DB_KEY = "DWH_BI1"

db_conn_props = get_connection_properties__by_key(SCOPE, DB_KEY)

print(db_conn_props["url"])

# COMMAND ----------

# bounds = get_bounds__by_rownum(db_dict=db_conn_props, table_name=f"{p_schema_name_source}.{p_table_name_source}")
# display(bounds)

# COMMAND ----------

# df = get_data_partitioned__by_rownum(db_dict=db_conn_props, table_name=f"{p_schema_name_source}.{p_table_name_source}", bounds=bounds)
df = get_data(db_dict=db_conn_props, table_name=f"{p_schema_name_source}.{p_table_name_source}")
display(df)

# COMMAND ----------

def perform_task(df, catalog_name, schema_name, table_name):
    # Construct the fully-qualified table name
    # dbx_qualified_table_name = f"{catalog_name}.{schema_name}.{table_name}"

    if not table_exists(catalog_name, schema_name, table_name):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} WITH DBPROPERTIES (Scope='{p_scope}')")

        print(f"writing: " + dbx_qualified_table_name)
        df.write.format("delta").mode("overwrite").saveAsTable(dbx_qualified_table_name)
        # print("\tOK")

        sql_pk_table = sql_pk_statement.format(**{"schema": schema_name, "table_name": table_name})
        # print(sql_pk_table)
        df_pk = get_data(db_dict=db_conn_props, table_name=sql_pk_table, query_type="query")
        for row_pk in df_pk.collect():
            column_name = row_pk["COLUMN_NAME"]
            sqls = [
                f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET NOT NULL",
                f"ALTER TABLE {dbx_qualified_table_name} DROP PRIMARY KEY IF EXISTS CASCADE",
                f"ALTER TABLE {dbx_qualified_table_name} ADD CONSTRAINT pk_{table_name}_{column_name} PRIMARY KEY({column_name})",
                f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')"
            ]
            for curr_sql in sqls:
                print("\t", curr_sql)
                spark.sql(curr_sql)
        result = create_status(status_code=201, status_message=f"CREATED: {dbx_qualified_table_name}")
        result["fqn"] = dbx_qualified_table_name
        return result
    else:
        result = create_status(status_code=208, status_message=f"SKIPPED: {dbx_qualified_table_name}") #208: already reported
        result["fqn"] = dbx_qualified_table_name
        return result

# COMMAND ----------


try:
    result = perform_task(df, p_catalog_name_target, p_schema_name_target, p_table_name_target)
except Exception as e:
    result = create_status(status_code=508, status_message="ERROR:" + str(e)) 
    result["fqn"] = dbx_qualified_table_name

dbutils.notebook.exit(result)
