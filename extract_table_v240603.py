# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import logging
import json
import pprint as pp
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
from helpers.db_helper_sql_oracle import (
    sql_pk_statement,
    sql_table_schema_statement,
    sql_top_distinct_columns_statement,
)
from helpers.logger_helper import log_to_delta
from helpers.status_helper import create_status

# COMMAND ----------

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
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
    job_id = str(notebook_info["tags"]["jobId"])
except Exception as e:
    print(__name__, e)
    job_id = "-1"
except:
    job_id = "-1"

# COMMAND ----------

DEBUG = False
if DEBUG:
    # p_work_json = {
    #     "pii": True,
    #     "scope": "ACC",
    #     "catalog_name_source": "",
    #     "schema_name_source": "STG",
    #     "table_name_source": "STG_EMOT_BTNCDHDR",
    #     "catalog_name": "acc__impetus_poc_pii",
    #     "schema_name": "stg",
    #     "table_name": "stg_emot_btncdhdr",
    #     "db_scope": "ACC",
    #     "db_key": "DWH_BI1__100000",
    #     "query_type": "dbtable",
    #     "query_sql": "",
    #     "mode": "overwrite",
    #     # "children": [],
    #     "run_ts": "2024-03-02T11:05:46.107518+00:00",
    #     "run_name": "/Users/sknecht@mazdaeur.com/.ide/databricks-mazda-ca32a17d/extract_table_runner_parallize_v240229",
    #     "fqn": "acc__impetus_poc_pii.stg.stg_emot_btncdhdr",
    #     "job_id": "766761984965330",
    # }
    # p_work_json = {'catalog_name': 'dev2__impetus_target', 'catalog_name_source': '', 'db_key': 'DWH_BI1__100000', 'db_scope': 'ACC', 'fqn': 'dev2__impetus_target.stg_tmp.v__temp_200_all_svc', 'mode': 'overwrite', 'pii': False, 'query_sql': '', 'query_type': 'dbtable', 'run_name': '/Repos/sknecht@mazdaeur.com/databricks-mazda/extract_table_runner_parallize', 'run_ts': '2024-04-05T09:46:12.053972+00:00', 'schema_name': 'stg_tmp', 'schema_name_source': 'STG_TMP', 'scope': 'DEV2', 'table_name': 'v__temp_200_all_svc', 'table_name_source': 'V$_TEMP_200_ALL_SVC', 'row_count': 26818087, 'partition_count': 24}
    # p_work_json = {'catalog_name': 'dev2__impetus_poc', 'catalog_name_source': '', 'db_key': 'DWH_BI1__100000', 'db_scope': 'ACC', 'fqn': 'dev2__impetus_poc.stg.stg_veh_master_btv14010', 'mode': 'overwrite', 'pii': False, 'query_sql': '', 'query_type': 'dbtable', 'run_name': '/Repos/sknecht@mazdaeur.com/databricks-mazda/extract_table_runner_parallize', 'run_ts': '2024-04-08T11:13:36.235945+00:00', 'schema_name': 'stg', 'schema_name_source': 'STG', 'scope': 'DEV2', 'table_name': 'stg_veh_master_btv14010', 'table_name_source': 'STG_VEH_MASTER_BTV14010', 'row_count': 7825615, 'partition_count': 24, 'job_id': '369886017422563'}
    p_work_json = {'catalog_name': 'dev2__impetus_poc', 'catalog_name_source': '', 'db_key': 'DWH_BI1__100000_COMP', 'db_scope': 'ACC', 'fqn': 'dev2__impetus_poc.stg.stg_dsr_vehicle_master', 'mode': 'overwrite', 'pii': False, 'query_sql': '', 'query_type': 'dbtable', 'run_name': '/Repos/sknecht@mazdaeur.com/databricks-mazda/extract_table_runner_parallize_compression', 'run_ts': '2024-04-08T16:14:35.676627+00:00', 'schema_name': 'stg', 'schema_name_source': 'STG', 'scope': 'DEV2', 'table_name': 'stg_dsr_vehicle_master', 'table_name_source': 'STG_DSR_VEHICLE_MASTER', 'row_count': 7824741, 'partition_count': 24}
    p_work_json = {"catalog_name": "dev_raw", "catalog_name_source": "", "db_key": "DWH_BI1__100000_COMP", "db_scope": "ACC", "fqn": "dev_raw.dwh.dim_dist", "mode": "overwrite", "partition_multiplier": 1, "pii": False, "query_sql": "", "query_type": "dbtable", "run_name": "/Repos/sknecht@mazdaeur.com/databricks-mazda/extract_table_runner_parallize", "run_ts": "2024-06-03T15:08:47.887904+00:00", "schema_name": "dwh", "schema_name_source": "DWH", "scope": "DEV", "table_name": "dim_dist", "table_name_source": "DIM_DIST", "action": "create", "row_count": 401, "partition_count": 1, "job_id": "707216970643174", "column_name_pks": "DIST_SK", "status_code": 500}
    p_work_json = {'catalog_name': 'dev_test', 'catalog_name_source': '', 'db_key': 'DWH_BI1__100000', 'db_scope': 'ACC', 'fqn': 'dev_test.lz_osb.event_details_files', 'mode': 'overwrite', 'partition_multiplier': 1, 'pii': False, 'query_sql': '', 'query_type': 'dbtable', 'run_name': '/Repos/sknecht@mazdaeur.com/databricks-mazda/extract_table_runner_parallize', 'run_ts': '2024-06-05T15:25:22.856542+00:00', 'schema_name': 'lz_osb', 'schema_name_source': 'lz_osb', 'scope': 'DEV', 'table_name': 'event_details_files', 'table_name_source': 'EVENT_DETAILS_FILES', 'row_count': 8, 'partition_count': 1}
else:
    p_work_json: dict = json.loads(dbutils.widgets.get("p_work_json"))
    assert p_work_json, "p_work_json not set"

print(p_work_json)

# COMMAND ----------

p_work_json["job_id"] = str(job_id)

p_scope = p_work_json["scope"]
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
print(p_scope, p_db_scope)
print(p_mode)

start_time = time.time()

# COMMAND ----------

print(p_work_json)

# COMMAND ----------

db_conn_props: dict = get_connection_properties__by_key(p_db_scope, p_db_key)

# merge p_work_json with db_conn_props (=overwrite)
work_item = {
    **p_work_json,
    "db_conn_props": db_conn_props,
}
work_item


# COMMAND ----------


class DelayedResultExtract:
    def __init__(self, work_item: dict, logger: logging.Logger = logger):
        self.start_time = time.time()
        self.exc_info = None
        self.result = None
        # self.action = work_item["action"]
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
        self.job_id = work_item["job_id"]
        self.logger = logger
        self.partition_count = work_item["partition_count"]
        self.row_count = work_item["row_count"]

    def do_work(self):
        try:

            # get all primary keys and indexes so that we can also apply it in the target table
            # we can also use this to determine a partition_key (if pk is a number and unique we can use it as partition key)
            sql_pk = sql_pk_statement.format(
                **{
                    "schema": self.schema_name_source,
                    "table_name": self.table_name_source,
                }
            )
            df_pk = get_jdbc_data_by_dict(
                db_conn_props=self.db_conn_props,
                work_item={
                    "query_sql": sql_pk,
                    "query_type": "query",
                },
            )
            column_name_pks = []
            column_name_partition = None
            schema_table_name_source = f"{self.schema_name_source}.{self.table_name_source}"

            for row in df_pk.collect():
                # we will use the first primary key as the primary key in the target table
                column_name_pks.append(row["COLUMN_NAME"])

                # can we also use it as partition key?
                if (
                    not column_name_partition
                    and self.query_type == "dbtable"
                    and row["DATA_TYPE"] in ("NUMBER", "DATE")
                ):
                    column_name_partition = row["COLUMN_NAME"]

            self.logger.debug(f"column_name_partition: '{column_name_partition}'")

            # method 2 to determine partition key, find the column with the most distinct values
            if not column_name_partition:
                sql_distinct = sql_top_distinct_columns_statement.format(
                    **{
                        "schema": self.schema_name_source,
                        "table_name":self.table_name_source,
                    }
                )
                df_distinct = get_jdbc_data_by_dict(
                    db_conn_props=self.db_conn_props,
                    work_item={
                        "query_sql": sql_distinct,
                        "query_type": "query",
                    },
                )
                for row in df_distinct.collect():
                    column_name_partition = row["COLUMN_NAME"]
                    break

            # correction for bad column data type translations
            # CHAR should remain CHAR not VARCHAR - PROBLEM: DBX does not have fixed size strings
            # DATE should remain DATE not TIMESTAMP
            # NUMBER should be INT if scale is 0
            sql_schema = sql_table_schema_statement.format(
                **{
                    "schema": self.schema_name_source,
                    "table_name": self.table_name_source,
                }
            )
            df_schema = get_jdbc_data_by_dict(
                db_conn_props=self.db_conn_props,
                work_item={
                    "query_sql": sql_schema,
                    "query_type": "query",
                },
            )

            df_schema.show(100, False)

            # the default data type conversion to DB data types is not always correct
            # we will use the data type translations to correct this
            customSchemas = []
            column_names = []

            self.logger.debug(f"customSchemas: '{customSchemas}'")


            for row in df_schema.collect():
                dbx_data_type = row["DBX_DATA_TYPE"]
                if dbx_data_type:
                    customSchemas.append(dbx_data_type)

                # # the CHAR issue is fixed by surrounding it with quotes
                # # this is handled by adapting the SELECT statement
                dbx_column_name = row["DBX_COLUMN_NAME"]
                if dbx_column_name:
                    column_names.append(dbx_column_name)

            try:

                # now that we have bounds, we can customSchema
                if customSchemas:
                    customSchema = ", ".join(customSchemas)
                    self.db_conn_props["customSchema"] = customSchema
                    self.logger.debug(f"customSchema: '{customSchema}'")

                if column_names:
                    column_names = ", ".join(column_names)
                    self.logger.debug(f"column_names: '{column_names}'")

                if not column_name_partition:

                    self.logger.warning(f"No partition key found: {self.table_name_source}")

                    df = get_jdbc_data_by_dict(
                        db_conn_props=self.db_conn_props,
                        work_item={
                            **self.work_item,
                            "table_sql": schema_table_name_source,
                        },
                    )
                    self.work_item["partition_count"] = self.partition_count

                else:
                    self.logger.info(f"Partition key found: {self.table_name_source}: {column_name_partition}")

                    bounds = get_jdbc_bounds__by_partition_key(
                        db_conn_props=self.db_conn_props,
                        table_name=schema_table_name_source,
                        column_name_partition=column_name_partition,
                    )


                    df = get_jdbc_data_by_dict__by_partition_key(
                        db_conn_props=self.db_conn_props,
                        work_item={
                            **self.work_item,
                            "table_sql": schema_table_name_source,
                        },
                        bounds=bounds,
                        column_name_partition=column_name_partition,
                        partition_count=self.partition_count,
                    )

                df.write.format("delta").mode(self.mode).option( "overwriteSchema", "true" ).saveAsTable(self.fqn)
                # .option( "overwriteSchema", "true" )

            # catch empty datasets
            except (PySparkException, Exception) as e:
                if hasattr(e, "getErrorClass"):
                    if e.getErrorClass():
                        self.logger.error(e.getErrorClass())
                        if e.getErrorClass() == "CANNOT_INFER_EMPTY_SCHEMA":
                            logger.warning("CANNOT_INFER_EMPTY_SCHEMA: DataFrame empty")
                            result = create_status(
                                scope=p_scope,
                                status_code=204,
                                status_message=f"NO_CONTENT: {self.fqn} resultset empty - 2: job {self.job_id}",
                                status_ctx=self.work_item,
                            )
                            result["row_count"] = 0
                            result["work_item"] = {
                                **self.work_item,
                                "table_sql": schema_table_name_source,
                            }
                            self.result = result
                            return
                    else:
                        self.logger.error(
                            "Unhandled Exception 1: " + str(type(e)) + str(e)
                        )
                        raise Exception(f"do_work Exception: Job{self.job_id}") from e
                else:
                    self.logger.error("Unhandled Exception 2: " + str(type(e)) + str(e))
                    raise Exception(f"do_work Exception: Job{self.job_id}") from e

            status_message = f"{self.mode}: {self.fqn}, part#: {self.partition_count} [{self.row_count}]"

            # if self.mode == "overwrite":

                # sqls = []

                # # add tags to the table for PII/confidential data
                # if self.work_item.get("pii", False):
                #     status_message += " with PII"
                #     sqls.append(f"ALTER TABLE {self.fqn} SET TAGS ('pii_table' = 'TRUE')")

                # replicate the primary keys and indexes we found in the source table
                # if column_name_pks:
                #     sqls.append(
                #         f"ALTER TABLE {self.fqn} DROP PRIMARY KEY IF EXISTS CASCADE"
                #     )
                #     column_pk_names = ", ".join(column_name_pks)  # can be a composite key

                #     for column_name in column_name_pks:
                #         sqls.append(
                #             f"ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET NOT NULL"
                #         )
                #         sqls.append(
                #             f"ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')"
                #         )

                #     sqls.append(
                #         f"ALTER TABLE {self.fqn} ADD CONSTRAINT pk_{self.table_name}_{column_name_pks[0]} PRIMARY KEY({column_pk_names})"
                #     )
                #     status_message += f" with PK ({column_pk_names})"

                # for curr_sql in sqls:
                #     logging.info(curr_sql)
                #     spark.sql(curr_sql)

            result = create_status(
                scope=p_scope,
                status_code=201,
                status_message=status_message,
                status_ctx=self.work_item,
            )
            result["row_count"] = self.row_count
            if column_name_pks:
                result["column_name_pks"] = ",".join(column_name_pks)
            self.result = result
            self.exc_info = None

        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            tb = traceback.TracebackException(exc_type, exc_value, exc_tb)
            stack_trace = traceback.format_exc(limit=2, chain=True)
            status_message = "".join(tb.format_exception_only())
            if not status_message:
                status_message = str(exc_type)

            status_message = f"INTERNAL_SERVER_ERROR: DelayedResultExtract: job {self.job_id}: {status_message}"

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
                status_message="INTERNAL_SERVER_ERROR: get_result: job "
                + self.job_id
                + ": "
                + str(e),
                status_ctx=self.work_item,
            )
            result["traceback"] = traceback
            print(traceback)
            result["time_duration"] = time_duration
            result["job_id"] = str(self.job_id)
            log_to_delta(result)
            return json.dumps(result)

        self.result["time_duration"] = time_duration
        logger.debug(f"result: {self.result}")
        log_to_delta(self.result)
        return json.dumps(self.result)


# COMMAND ----------

result = None
try:
    # we will create the target and the work item is not append mode

    if p_mode != "append":
        # table should not yet exist, if it does we will skip this and return to caller
        if table_exists(p_catalog_name, p_schema_name, p_table_name):
            result: dict = create_status(
                scope=p_scope,
                status_code=208,
                status_message="ALREADY_REPORTED: table already exists - skipping",
                status_ctx=work_item,
            )

        # spark.sql(f"USE CATALOG {p_catalog_name}")
        # spark.sql(f"USE DATABASE {p_schema_name}")

    # we are creating the target and the work item is in append mode
    elif p_mode == "append":

        # because we are in append mode we need to check if the table exists
        if not table_exists(p_catalog_name, p_schema_name, p_table_name):
            result: dict = create_status(
                scope=p_scope,
                status_code=500,
                status_message="INTERNAL_SERVER_ERROR: Append table does not yet exists",
                status_ctx=work_item,
            )
            end_time = time.time()
            time_duration = int(end_time - start_time)
            result["time_duration"] = time_duration

except (Exception, AnalysisException) as e:
    exc_type, exc_value, exc_tb = sys.exc_info()
    tb = traceback.TracebackException(exc_type, exc_value, exc_tb)
    stack_trace = traceback.format_exc(limit=2, chain=True)
    status_message = "".join(tb.format_exception_only())
    if not status_message:
        status_message = str(e)

    # remove the JVM stacktrace - to focus on python errors
    if "JVM stacktrace" in stack_trace:
        stack_trace = stack_trace.split("JVM stacktrace:")[0]
    if "JVM stacktrace" in status_message:
        status_message = status_message.split("JVM stacktrace:")[0]

    result = create_status(
        scope=p_scope,
        status_code=500,
        status_message=f"INTERNAL_SERVER_ERROR: table_exists in prepare: {job_id}: {status_message}",
        status_ctx=work_item,
    )
    end_time = time.time()
    time_duration = int(end_time - start_time)
    result["time_duration"] = time_duration
    result["stack_trace"] = stack_trace

    log_to_delta(result)

    dbutils.notebook.exit(json.dumps(result))

if result:
    # there was no exception but we encountered a condition which allows us to quit early
    # during create we found that table already exists, etc.
    end_time = time.time()
    time_duration = int(end_time - start_time)
    result["time_duration"] = time_duration
    log_to_delta(result)
    logger.info(pp.pformat(result))
    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

result = None
try:
    dr = DelayedResultExtract(work_item=work_item)
    dr.do_work()

    result: str = dr.get_result()

    logger.info(result)

    assert type(result) == str, "result is not a string"

except (Exception, AnalysisException) as e:
    exc_type, exc_value, exc_tb = sys.exc_info()
    tb = traceback.TracebackException(exc_type, exc_value, exc_tb)
    stack_trace = traceback.format_exc(limit=2, chain=True)
    print(stack_trace)
    status_message = "".join(tb.format_exception_only())
    if not status_message:
        status_message = str(e)

    # remove the JVM stacktrace - to focus on python errors
    if "JVM stacktrace" in stack_trace:
        stack_trace = stack_trace.split("JVM stacktrace:")[0]
    if "JVM stacktrace" in status_message:
        status_message = status_message.split("JVM stacktrace:")[0]

    result = create_status(
        scope=p_scope,
        status_code=500,
        status_message=f"INTERNAL_SERVER_ERROR: DelayedResultExtract (OUT): {job_id}: {status_message} {e}",
        status_ctx=work_item,
    )
    result["stack_trace"] = stack_trace
    end_time = time.time()
    time_duration = int(end_time - start_time)
    result["time_duration"] = time_duration

    log_to_delta(result)
    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

if result:
    print(result)
    print(type(result))
    dbutils.notebook.exit(result)
