# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import logging
import json
import sys
import time
import traceback

from pyspark.errors import PySparkException
from pyspark.sql.utils import AnalysisException

from helpers.db_helper_delta import table_exists
from helpers.db_helper_jdbc import (
    get_connection_properties__by_key,
    get_jdbc_bounds__by_partition_key,
    get_jdbc_data_by_dict,
    get_jdbc_data_by_dict__by_partition_key,
)
from helpers.db_helper_sql_oracle import sql_pk_statement
from helpers.logger_helper import log_to_delta_table
from helpers.status_helper import create_status

# COMMAND ----------

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# COMMAND ----------

dbutils.widgets.text("p_work_json", "{}", label="Database Table Extract JSON Config")

# COMMAND ----------

try:
    notebook_info = json.loads(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    )
    # The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...)
    job_id = notebook_info["tags"]["jobId"]
except:
    job_id = -1

p_work_json: dict = json.loads(dbutils.widgets.get("p_work_json"))
assert p_work_json, "p_work_json not set"

p_work_json["job_id"] = job_id

p_scope = p_work_json["scope"]
p_action = p_work_json["action"]
p_db_scope = p_work_json["db_scope"]
p_db_key = p_work_json["db_key"]
p_catalog_name_source = p_work_json["catalog_name_source"]
p_schema_name_source = p_work_json["schema_name_source"]
p_table_name_source = p_work_json["table_name_source"]
p_catalog_name = p_work_json["catalog_name"]
p_schema_name = p_work_json["schema_name"]
p_table_name = p_work_json["table_name"]
p_mode = p_work_json.get("mode", "overwrite")
p_fqn = p_work_json.get("fqn", f"{p_catalog_name}.{p_schema_name}.{p_table_name}")

logger.info(p_fqn)
logger.info(job_id)
print(p_scope, p_db_scope)
print(p_action)
print(p_mode)

start_time = time.time()

# COMMAND ----------

db_conn_props: dict = get_connection_properties__by_key(p_db_scope, p_db_key)

# merge p_work_json with db_conn_props (=overwrite)
work_item = {
    **p_work_json,
    "db_conn_props": db_conn_props,
}

# COMMAND ----------

actions = p_action.split("__")

# drop the table if action or mode matches
# if action is only drop than we don't want append to run. drop and quit
if ("drop" in actions and p_mode != "append") or p_mode == "drop" or p_action == "drop":
    spark.sql(f"DROP TABLE IF EXISTS {p_fqn}")

    if p_mode == "drop" or p_action == "drop":
        # drop was the only thing to do, let's quit
        dbutils.notebook.exit(
            json.dumps(
                {
                    "job_id": job_id,
                    "fqn": p_fqn,
                    "status_code": 200,
                    "status_message": "OK",
                }
            )
        )

# bounds = get_bounds__by_rownum(db_dict=db_conn_props, table_name=f'{p_schema_name_source}.{p_table_name_source}')
# display(bounds)

# COMMAND ----------


class DelayedResultExtract:
    def __init__(self, work_item: dict, logger: logging.Logger = logger):
        self.start_time = time.time()
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
        self.partition_count = work_item.get("partition_count", 4)
        self.logger = logger

    def do_work(self):
        try:
            # logger.info(pformat(self.work_item))

            # get all primary keys and indexes so that we can also apply it in the target table
            # we can also use this to determine a partition_key (if pk is a number and unique we can use it as partition key)
            sql_pk = sql_pk_statement.format(
                **{
                    "schema": self.schema_name_source,
                    "table_name": self.table_name_source,
                }
            )
            df_pk = get_jdbc_data_by_dict(
                db_conn_props=db_conn_props,
                work_item={
                    "query_sql": sql_pk,
                    "query_type": "query",
                },
            ).load()
            column_name_pks = []
            column_name_partition = None
            table_name_source = f"{self.schema_name_source}.{self.table_name_source}"

            for row_pk in df_pk.collect():
                # we will use the first primary key as the primary key in the target table
                column_name_pks.append(row_pk["COLUMN_NAME"])

                # can we also use it as partition key? This is only available when we ingest tables, not queries
                if self.query_type  == "dbtable" and row_pk["DATA_TYPE"] == "NUMBER":
                    column_name_partition = row_pk["COLUMN_NAME"]

            # if df.count() == 0:
            #     result = create_status(
            #         scope=p_scope,
            #         status_code=204,
            #         status_message=f"NO_CONTENT: {self.fqn} resultset empty",
            #         status_ctx=work_item
            #     )
            #     result["row_count"] = 0
            #     result["work_item"] = {
            #         **self.work_item,
            #         "table_sql": f"{self.schema_name_source}.{self.table_name_source}",
            #     }
            #     self.result = result
            #     return

            try:

                if column_name_partition:
                    bounds = get_jdbc_bounds__by_partition_key(
                        db_conn_props=db_conn_props,
                        table_name=table_name_source,
                        column_name_partition=column_name_partition,
                    )
                    range_size = bounds.MAX_ID - bounds.MIN_ID

                    bin_size = 500_000
                    if range_size < bin_size:
                        self.partition_count = 1
                    else:
                        # we want to have at least 100_000 rows per partition with a maxum of self.partition_count
                        self.partition_count = min(self.partition_count, int(range_size / bin_size))

                    logger.info(f"Partitioning table {table_name_source} by {column_name_partition} into {self.partition_count} partitions")

                    df = get_jdbc_data_by_dict__by_partition_key(
                        db_conn_props=db_conn_props,
                        work_item={
                            **self.work_item,
                            "table_sql": table_name_source,
                            "query_type" : 'dbtable' # with partitioning query is not allowed
                        },
                        bounds=bounds,
                        column_name_partition=column_name_partition,
                        partition_count=self.partition_count
                    ).load()

                else:
                    df = get_jdbc_data_by_dict(
                        db_conn_props=db_conn_props,
                        work_item={
                            **self.work_item,
                            "table_sql": table_name_source,
                        },
                    ).load()

                df.write.format("delta").mode(self.mode).saveAsTable(self.fqn)

            # catch empty datasets
            except PySparkException as e:
                self.logger.error(e.getErrorClass())
                if e.getErrorClass() == "CANNOT_INFER_EMPTY_SCHEMA":
                    logger.warning("CANNOT_INFER_EMPTY_SCHEMA: DataFrame empty")
                    result = create_status(
                        scope=p_scope,
                        status_code=204,
                        status_message=f"NO_CONTENT: {self.fqn} resultset empty",
                        status_ctx=self.work_item,
                    )
                    result["row_count"] = 0
                    result["work_item"] = {
                        **self.work_item,
                        "table_sql": table_name_source,
                    }
                    self.result = result
                    return
                else:
                    self.logger.error("Unhandled PySparkException: " + e.getErrorClass())
                    raise


            status_message = f"{self.mode}: {self.fqn}"

            if self.mode == "overwrite":

                sqls = []

                # add tags to the table for PII/confidential data
                if self.work_item.get("pii", False):
                    status_message += " with PII"
                    sqls.append(f"ALTER TABLE {self.fqn} SET TAGS ('pii_table' = 'TRUE')")

                # replicate the primary keys and indexes we found in the source table
                if column_name_pks:
                    sqls.append(f"ALTER TABLE {self.fqn} DROP PRIMARY KEY IF EXISTS CASCADE")
                    column_pk_names = ", ".join(column_name_pks) # can be a composite key

                    for column_name in column_name_pks:
                        sqls.append(f"ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET NOT NULL")
                        sqls.append(f"ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')")

                    sqls.append(f"ALTER TABLE {self.fqn} ADD CONSTRAINT pk_{self.table_name}_{column_name_pks[0]} PRIMARY KEY({column_pk_names})")
                    status_message += f" with primary key ({column_pk_names})"

                for curr_sql in sqls:
                    spark.sql(curr_sql)

            result = create_status(
                scope=p_scope,
                status_code=201,
                status_message=status_message,
                status_ctx=self.work_item,
            )
            result["row_count"] = df.count()
            self.result = result
            self.exc_info = None
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

    def get_result(self) -> str:
        end_time = time.time()
        time_duration = int(end_time - self.start_time)
        if self.exc_info:
            e, traceback = self.exc_info

            result = create_status(
                scope=p_scope,
                status_code=500,
                status_message="INTERNAL_SERVER_ERROR:" + str(e),
                status_ctx=self.work_item,
            )
            result["traceback"] = traceback
            result["time_duration"] = time_duration
            result["job_id"] = job_id
            log_to_delta_table(result)
            return json.dumps(result)

        self.result["time_duration"] = time_duration
        logger.info(f"result: {self.result}")
        log_to_delta_table(self.result)
        return json.dumps(self.result)


# COMMAND ----------

try:
    # we will create the target and the work item is not append mode

    if "create" in actions and p_mode != "append":
        # table should not yet exist, if it does we will skip this and return to caller
        if table_exists(p_catalog_name, p_schema_name, p_table_name):
            result = create_status(
                scope=p_scope,
                status_code=208,
                status_message="ALREADY_REPORTED: table already exists - skipping",
                status_ctx=work_item,
            )

            log_to_delta_table(result)
            dbutils.notebook.exit(json.dumps(result))

        # all is good. Let's create the catalog and schema the table will be in
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {p_catalog_name}.{p_schema_name} WITH DBPROPERTIES (scope='{p_scope}')"
        )

    # we are creating the target and the work item is in append mode
    elif "create" in actions and p_mode == "append":

        # # first action is a drop_create (using df overwrite). This might take some time to finish so we need to give it some time before we check if the NEW table is created
        # # TODO: this is a possible race condition that we need to fix in future
        # time.sleep(180)

        # counter = 0
        # while not table_exists(p_catalog_name, p_schema_name, p_table_name):
        #     time.sleep(60)
        #     counter += 1
        #     if counter > 30:
        #         # TODO: Improve: longer is theoretically not possible because we also have a timeout from calling notebook
        #         break

        # because we are in append mode we need to check if the table exists
        if not table_exists(p_catalog_name, p_schema_name, p_table_name):
            result = create_status(
                scope=p_scope,
                status_code=500,
                status_message="INTERNAL_SERVER_ERROR: Append table does not yet exists",
                status_ctx=work_item,
            )

            log_to_delta_table(result)
            dbutils.notebook.exit(json.dumps(result))

except (Exception, AnalysisException) as e:
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
        scope=p_scope,
        status_code=500,
        status_message=f"INTERNAL_SERVER_ERROR: table_exists in prepare: {status_message}",
        status_ctx=work_item,
    )
    result["stack_trace"] = stack_trace

    log_to_delta_table(result)

    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

result = None
try:
    dr = DelayedResultExtract(work_item=work_item)
    dr.do_work()

    result: str = dr.get_result()

    logger.info(result)

    assert type(result) == str, "result is not a string"

    log_to_delta_table(json.loads(result))

except (Exception, AnalysisException) as e:
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
        scope=p_scope,
        status_code=500,
        status_message=f"INTERNAL_SERVER_ERROR: table_exists in DelayedResultExtract: {status_message} {e}",
        status_ctx=work_item,
    )
    result["stack_trace"] = stack_trace

    log_to_delta_table(result)
    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

if result:
    dbutils.notebook.exit(result)
