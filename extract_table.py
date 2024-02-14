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

dbutils.widgets.dropdown(
    "p_scope", "ACC", choices=["ACC", "PRD"], label="Development Scope"
)
dbutils.widgets.text(
    "p_catalog_name_target", "impetus_ref", label="Catalog Name Target"
)
dbutils.widgets.text("p_schema_name_source", "STG", label="Schema Name Source")
dbutils.widgets.text(
    "p_table_name_source", "STG_LEM_TRANSPORT_MODES", label="Table Name Source"
)

# COMMAND ----------

start_time = time.time()
p_scope = dbutils.widgets.get("p_scope")
p_catalog_name_target = dbutils.widgets.get("p_catalog_name_target").lower()
p_schema_name_target = dbutils.widgets.get("p_schema_name_source").lower()
p_table_name_target = dbutils.widgets.get("p_table_name_source").lower().replace("$", "_")
p_schema_name_source = dbutils.widgets.get("p_schema_name_source")
p_table_name_source = dbutils.widgets.get("p_table_name_source")


dbx_qualified_table_name = (
    f"{p_catalog_name_target}.{p_schema_name_target}.{p_table_name_target}"
)
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


class DelayedResult:
    def __init__(self):
        self.exc_info = None
        self.result = None
        self.fqn = None

    def test_exception(self):
        # raise Exception("This is a test exception")
        try:
            raise Exception("This is a test exception")
        except Exception as e:
            # import sys
            self.exc_info = (e, traceback.format_exc())

    def do_work(self, df, catalog_name, schema_name, table_name):
        self.fqn = f"{catalog_name}.{schema_name}.{table_name}"
        try:
            spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} WITH DBPROPERTIES (Scope='{p_scope}')"
            )

            print(f"writing: " + dbx_qualified_table_name)
            df.write.format("delta").mode("overwrite").saveAsTable(
                dbx_qualified_table_name
            )
            # print("\tOK")

            sql_pk_table = sql_pk_statement.format(
                **{"schema": schema_name, "table_name": table_name}
            )
            # print(sql_pk_table)
            df_pk = get_data(
                db_dict=db_conn_props,
                table_name=sql_pk_table,
                query_type="query",
            )
            for row_pk in df_pk.collect():
                column_name = row_pk["COLUMN_NAME"]
                sqls = [
                    f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET NOT NULL",
                    f"ALTER TABLE {dbx_qualified_table_name} DROP PRIMARY KEY IF EXISTS CASCADE",
                    f"ALTER TABLE {dbx_qualified_table_name} ADD CONSTRAINT pk_{table_name}_{column_name} PRIMARY KEY({column_name})",
                    f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')",
                ]
                for curr_sql in sqls:
                    # print("\t", curr_sql)
                    spark.sql(curr_sql)
            result = create_status(
                status_code=201, status_message=f"CREATED: {self.fqn}"
            )
            result["fqn"] = self.fqn
            result["row_count"] = df.count()
            self.result = result
        except Exception as e:
            self.exc_info = (e, sys.exc_info())

    def get_result(self):
        end_time = time.time()
        time_duration = int(end_time - start_time)
        # print(f"Execution time: {execution_time} seconds")
        if self.exc_info:
            # Rethrow the exception using sys.exc_info()
            e, traceback = self.exc_info
            # _, _, traceback = exc_info

            result = create_status(
                status_code=508, status_message="ERROR:" + str(e)
            )
            result["fqn"] = dbx_qualified_table_name
            result["traceback"] = traceback
            result["time_duration"] = time_duration
            return json.dumps(result)

            # raise e.with_traceback(traceback)
        # raise self.exc_info[1], None, self.exc_info[2]
        self.result["time_duration"] = time_duration
        return json.dumps(self.result)

# COMMAND ----------

    
# dr = DelayedResult()
# dr.test_exception()
# dbutils.notebook.exit(dr.get_result())

# COMMAND ----------

if not table_exists(
    p_catalog_name_target, p_schema_name_target, p_table_name_target
):
    # df = get_data_partitioned__by_rownum(db_dict=db_conn_props, table_name=f"{p_schema_name_source}.{p_table_name_source}", bounds=bounds)
    df = get_data(
        db_dict=db_conn_props,
        table_name=f"{p_schema_name_source}.{p_table_name_source}",
    )

    # try:
    dr = DelayedResult()
    dr.do_work(
        df, p_catalog_name_target, p_schema_name_target, p_table_name_target
    )

    dbutils.notebook.exit(dr.get_result())
else:
    result = create_status(
        status_code=208, status_message=f"SKIPPED: {dbx_qualified_table_name}"
    )
    # 208: already reported
    result["fqn"] = dbx_qualified_table_name
    end_time = time.time()
    time_duration = int(end_time - start_time)
    result["time_duration"] = time_duration
    dbutils.notebook.exit(json.dumps(result))
