# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import logging
import json
import sys
import time
import traceback

from pyspark.sql.utils import AnalysisException

from helpers.db_helper import (
    get_bounds__by_rownum,
    get_connection_properties__by_key,
    get_data_partitioned__by_rownum,
    get_db_dict,
    get_jdbc_data_by_dict,
    table_exists,
)
from helpers.db_helper_oracle_sql import sql_pk_statement
from helpers.status_helper import create_status

# COMMAND ----------

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.debug("About to do something")
logger.info("This is an informational message")

# COMMAND ----------

dbutils.widgets.text("p_work_json", "{}", label="Database Table Extract JSON Config")

# COMMAND ----------

start_time = time.time()

p_work_json: dict = json.loads(dbutils.widgets.get("p_work_json"))
assert p_work_json, "p_work_json not set"

p_scope = p_work_json["scope"]
p_action = p_work_json["action"]
p_db_key = p_work_json["db_key"]
p_catalog_name_source = p_work_json["catalog_name_source"]
p_schema_name_source = p_work_json["schema_name_source"]
p_table_name_source = p_work_json["table_name_source"]
p_catalog_name = p_work_json["catalog_name"]
p_schema_name = p_work_json["schema_name"]
p_table_name = p_work_json["table_name"]
p_mode = p_work_json.get("mode", "overwrite")
p_fqn = p_work_json.get("fqn", f"{p_catalog_name}.{p_schema_name}.{p_table_name}")

print(p_fqn)
print(p_scope)
print(p_action)
print(p_mode)

# COMMAND ----------

db_conn_props: dict = get_connection_properties__by_key(p_scope, p_db_key)
work_item = {
    **p_work_json,
    "db_conn_props": db_conn_props,
}  # merge p_work_json with db_conn_props (=overwrite)

# print(db_conn_props['url'])

# COMMAND ----------

actions = p_action.split("__")

# drop the table if it exists
if ("drop" in actions and p_mode != "append") or p_mode == "drop":
    # in case we are appending we might need to drop table first
    spark.sql(f"DROP TABLE IF EXISTS {p_fqn}")
    if p_mode == "drop":
        result = create_status(
            status_code=200,
            status_message="OK: table dropped",
        )
        result["fqn"] = p_fqn
        end_time = time.time()
        time_duration = int(end_time - start_time)
        result["time_duration"] = time_duration
        dbutils.notebook.exit(json.dumps(result))

# dbutils.notebook.exit(json.dumps(work_item))

# bounds = get_bounds__by_rownum(db_dict=db_conn_props, table_name=f'{p_schema_name_source}.{p_table_name_source}')
# display(bounds)

# COMMAND ----------


class DelayedResultExtract:
    def __init__(self, work_item: dict):
        self.exc_info = None
        self.result = None
        self.action = work_item["action"]
        self.catalog_name_source = work_item["catalog_name_source"]
        self.schema_name_source = work_item["schema_name_source"]
        self.table_name_source = work_item["table_name_source"]
        self.catalog_name = work_item["catalog_name"]
        self.schema_name = work_item["schema_name"]
        self.table_name = work_item["table_name"]
        self.query_type = work_item["query_type"]
        self.query_sql = work_item["query_sql"]
        self.scope = work_item["scope"]
        self.db_conn_props = work_item["db_conn_props"]
        self.work_item = work_item
        self.mode = work_item["mode"]
        self.fqn = work_item["fqn"]

    def do_work(self):
        try:

            # get all primary keys and indexes so that we can also apply it in the target table
            pk_sql = sql_pk_statement.format(
                **{
                    "schema": self.schema_name_source,
                    "table_name": self.table_name_source,
                }
            )
            df_pk = get_jdbc_data_by_dict(
                db_conn_props=db_conn_props,
                work_item={
                    "query_sql": pk_sql,
                    "query_type": "query",
                },
            )

            df = get_jdbc_data_by_dict(
                db_conn_props=db_conn_props,
                work_item={
                    **self.work_item,
                    "table_sql": f"{self.schema_name_source}.{self.table_name_source}",
                },
            )

            if df.count() == 0:
                result = create_status(
                    status_code=204,
                    status_message=f"NO_CONTENT: {self.fqn} resultset empty",
                )
                result["fqn"] = self.fqn
                result["row_count"] = 0
                result["work_item"] = {
                    **self.work_item,
                    "table_sql": f"{self.schema_name_source}.{self.table_name_source}",
                }
                self.result = result
                return

            df.write.format("delta").mode(self.mode).saveAsTable(self.fqn)

            # replicate the primary keys and indexes we found in the source table
            if self.mode == "overwrite":
                for row_pk in df_pk.collect():
                    column_name = row_pk["COLUMN_NAME"]
                    sqls = [
                        f"ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET NOT NULL",
                        f"ALTER TABLE {self.fqn} DROP PRIMARY KEY IF EXISTS CASCADE",
                        f"ALTER TABLE {self.fqn} ADD CONSTRAINT pk_{self.table_name}_{column_name} PRIMARY KEY({column_name})",
                        f"ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')",
                    ]
                    for curr_sql in sqls:
                        spark.sql(curr_sql)

            result = create_status(status_code=201, status_message=f"CREATED: {self.fqn}")
            result["fqn"] = self.fqn
            result["row_count"] = df.count()
            self.result = result
        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            tb = traceback.TracebackException(exc_type, exc_value, exc_tb)
            stack_trace = traceback.format_exc(limit=2, chain=True)
            status_message = "".join(tb.format_exception_only())

            # remove the JVM stacktrace - to focus on python errors
            if "JVM stacktrace" in stack_trace:
                stack_trace = stack_trace.split("JVM stacktrace:")[0]
            if "JVM stacktrace" in status_message:
                status_message = status_message.split("JVM stacktrace:")[0]

            self.exc_info = (
                status_message,
                stack_trace,
            )

    def get_result(self):
        end_time = time.time()
        time_duration = int(end_time - start_time)
        if self.exc_info:
            e, traceback = self.exc_info

            result = create_status(
                status_code=500, status_message="INTERNAL_SERVER_ERROR:" + e
            )
            result["fqn"] = self.fqn
            result["traceback"] = traceback
            result["time_duration"] = time_duration
            return json.dumps(result)

        self.result["time_duration"] = time_duration
        logger.info(f"result: {self.result}")
        return json.dumps(self.result)


# COMMAND ----------

# if "create" in actions:
#     # table_exists requires the catalog to exist, so let's create it first
#     spark.sql(
#         f"CREATE CATALOG IF NOT EXISTS {p_catalog_name} WITH DBPROPERTIES (scope='{p_scope}')"
#     )

try:
    # we will create the target and the work item is not append mode

    if "create" in actions and p_mode != "append":
        # table should not yet exist, if it does we will skip this and return to caller
        if table_exists(p_catalog_name, p_schema_name, p_table_name):
            result = create_status(
                status_code=208,
                status_message="ALREADY_REPORTED: table already exists - skipping",
            )
            result["fqn"] = p_fqn
            end_time = time.time()
            time_duration = int(end_time - start_time)
            result["time_duration"] = time_duration
            dbutils.notebook.exit(json.dumps(result))

        # all is good. Let's create the catalog and schema the table will be in
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {p_catalog_name}.{p_schema_name} WITH DBPROPERTIES (scope='{p_scope}')"
        )

    # we are creating the target and the work item is in append mode
    elif "create" in actions and p_mode == "append":

        counter = 0
        while not table_exists(p_catalog_name, p_schema_name, p_table_name):
            time.sleep(60)
            counter += 1
            if counter > 10:
                break

        # because we are in append mode we need to check if the table exists
        if not table_exists(p_catalog_name, p_schema_name, p_table_name):
            result = create_status(
                status_code=500,
                status_message="INTERNAL_SERVER_ERROR: Append table does not yet exists",
            )
            # 208: already reported
            result["fqn"] = p_fqn
            end_time = time.time()
            time_duration = int(end_time - start_time)
            result["time_duration"] = time_duration
            dbutils.notebook.exit(json.dumps(result))

except AnalysisException as e:
    exc_type, exc_value, exc_tb = sys.exc_info()
    tb = traceback.TracebackException(exc_type, exc_value, exc_tb)
    stack_trace = traceback.format_exc(limit=2, chain=True)
    status_message = "".join(tb.format_exception_only())

    # remove the JVM stacktrace - to focus on python errors
    if "JVM stacktrace" in stack_trace:
        stack_trace = stack_trace.split("JVM stacktrace:")[0]
    if "JVM stacktrace" in status_message:
        status_message = status_message.split("JVM stacktrace:")[0]

    result = create_status(
        status_code=500,
        status_message="INTERNAL_SERVER_ERROR: table_exists: {status_message}",
    )
    result["fqn"] = p_fqn
    result["stack_trace"] = stack_trace

    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

# try:
dr = DelayedResultExtract(work_item=work_item)
dr.do_work()

# COMMAND ----------

dbutils.notebook.exit(dr.get_result())
