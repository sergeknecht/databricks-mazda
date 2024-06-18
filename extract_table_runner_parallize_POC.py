# Databricks notebook source
# MAGIC %md
# MAGIC # Data Extraction Runner Oracle to Delta Lake
# MAGIC ## Can run in parallel in refresh (drop and re-create) or create mode (if not exists)

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

import logging
import asyncio
import copy
import datetime
import json
import pprint as pp
import time
from math import ceil
from queue import Queue
from threading import Thread

import pyspark.sql.functions as F

from helpers.app_helper import init
from helpers.asyncio_helper import run_parallel, run_sequence
from helpers.db_helper_delta import has_table
from helpers.db_helper_jdbc import (
    get_connection_properties__by_key,
    get_jdbc_data_by_dict,
)
from helpers.logger_helper import log_to_delta
from helpers.oracledb_helper import do_parallel_query
from helpers.status_helper import create_status

# COMMAND ----------

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Definition Using Widgets

# COMMAND ----------

dbutils.widgets.dropdown(
    "jp_action", "CREATE", ["DROP", "CREATE", "DROP__CREATE"], label="Action to perform"
)
dbutils.widgets.dropdown(
    "jp_stop_on_exception",
    "TRUE",
    ["TRUE", "FALSE"],
    label="raise exception on data error",
)
dbutils.widgets.dropdown(
    "jp_scope",
    "DEV",
    ["DEV", "UAT", "PRD", "TST"],
    label="UC catalog prefix (=scope)",
)
dbutils.widgets.dropdown(
    "p_db_key",
    "DWH_BI1__100000_COMP",
    [
        "DWH_BI1__100000_COMP",
        "DWH_BI1__100000",
        "DWH_BI1",
        "DWH_BI1__500000",
        "DWH_BI1__250000",
    ],
    label="DB Config to use",
)
dbutils.widgets.dropdown(
    "jp_db_scope",
    "ACC",
    ["DVL", "ACC", "PRD", "TST"],
    label="where to read the Oracle DB data from",
)
dbutils.widgets.text(
    "jp_worker_count",
    "",
    label="workers accross all nodes, partitioning will create more tasks",
)
dbutils.widgets.text(
    "jp_partition_count_max",
    "",
    label="max number of partitions used by JDBC when reading tables",
)
dbutils.widgets.text(
    "jp_work_config_filename", "", label="filename.json containing the work items"
)


# COMMAND ----------

jp_action: str = dbutils.widgets.get("jp_action").upper()
jp_stop_on_exception: bool = dbutils.widgets.get("jp_stop_on_exception").upper() == "TRUE"
jp_action + "," + str(jp_stop_on_exception)
jp_scope: str = dbutils.widgets.get("jp_scope")
p_db_key: str = dbutils.widgets.get("p_db_key")
jp_db_scope: str = dbutils.widgets.get("jp_db_scope")
jp_worker_count = dbutils.widgets.get("jp_worker_count")
jp_partition_count_max = dbutils.widgets.get("jp_partition_count_max")
jp_work_config_filename = dbutils.widgets.get("jp_work_config_filename")


# defaults
if not jp_worker_count:
    jp_worker_count = None
else:
    jp_worker_count = int(jp_worker_count)
if not jp_partition_count_max:
    jp_partition_count_max = None
else:
    jp_partition_count_max = int(jp_partition_count_max)
if not jp_work_config_filename:
    jp_work_config_filename = (
        "clone_tables__impetus_src.json" or "work_items__impetus_poc.json"
    )


# COMMAND ----------

DEBUG = False
if DEBUG:
    jp_action = "CREATE"
    jp_worker_count = 30
    jp_partition_count_max = 20
    jp_work_config_filename = "clone_tables__impetus_src.json"

# COMMAND ----------

# JOB PARAMETERS
jp_actions = jp_action.split("__")
jp_run_version = "v240603"  # version of the job
run_ts = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
run_name = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)

# workers accross all nodes, but partitioning will create more tasks
# however we limit it to max 20 partitions per query
# total lis 128 CPUs, therefore workers should be limited to 128 cpu's / 20 partitions
# to get max number of workers
worker_count = jp_worker_count or int(sc.defaultParallelism * 0.95)
MAX_PARTITIONS = jp_partition_count_max or (8 * 3)

print(p_db_key, jp_action, worker_count, MAX_PARTITIONS)

timeout_sec = 3600  # 1:00 hours

start_time = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the work items (tables to load) from json file

# COMMAND ----------

with open(f"./config/{jp_work_config_filename}") as f:
    work_jsons = json.load(f)

print(len(work_jsons))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize logging to Delta Application Log Table

# COMMAND ----------

app_status = init(jp_scope)
display(app_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Based on job parameters and loaded json file build the work items (a list of dictionaries)

# COMMAND ----------

for wi in work_jsons:
    assert "pii" in wi, f"pii not found in {wi}"
    assert "catalog" in wi, f"catalog not found in {wi}"
    assert "name" in wi, f"name not found in {wi}"


def get_schema_name_source(x: dict) -> str:
    schema = x["name"].split(".")[0]
    if schema == "STAGING":
        return "STG"
    elif schema == "LANDING_ZONE_LEMANS":
        return "LZ_LEM"
    elif schema == "STG":
        return "STG"
    elif schema == "STAGING_TEMP":
        return "STG_TMP"
    elif schema in ("STG", "LZ_LEM", "STG_TMP", "DWH"):
        return schema
    else:
        return schema.lower()


get_table_name_source = lambda x: x["name"].split(".")[1]


def get_catalog_name(wi: dict) -> str:
    return f"{jp_scope.lower()}_{wi['catalog']}"
    # if not wi["pii"]
    # else f"{jp_scope.lower()}_{wi['catalog']}_pii"


def create_work_item(wi: dict) -> dict:
    wi = {
        "pii": wi["pii"],
        "scope": jp_scope,
        "catalog_name_source": "",
        "schema_name_source": get_schema_name_source(wi),
        "table_name_source": get_table_name_source(wi),
        "catalog_name": get_catalog_name(wi),
        "schema_name": get_schema_name_source(wi).lower().replace("$", "_"),
        "table_name": get_table_name_source(wi).lower().replace("$", "_"),
        "db_scope": jp_db_scope,
        "db_key": wi.get("db_key", p_db_key),
        "query_type": wi.get("query_type", "dbtable"),
        "query_sql": wi.get("query_sql", ""),
        "mode": wi.get("mode", "overwrite"),
        # "children": wi.get("children", []),
        "run_ts": run_ts,
        "run_name": run_name,
        "partition_multiplier": wi.get("partition_multiplier", 1),
    }
    wi["fqn"] = f"{wi['catalog_name']}.{wi['schema_name']}.{wi['table_name']}"
    return wi


work_items = [
    create_work_item(wi) | {"__query_id__": idx} for idx, wi in enumerate(work_jsons)
]

print(len(work_items))

# COMMAND ----------

work_items[:2]

# COMMAND ----------

df = spark.createDataFrame(work_items)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP tables in target location (delta) if it exist


# MAGIC %md
# MAGIC ## TEST TO DO - write drop commands in poc_multiple_sqls.sql and run notebook. should be MUCH FASTER

# COMMAND ----------

result: str = dbutils.notebook.run(
    f"poc_sql_notebook_writer",
    timeout_sec,
    # {
    #     "p_work_json": json.dumps(work_item),
    # },
)
print(result)


# COMMAND ----------


if "DROP" in jp_actions:

    async def drop_table_delta(row) -> dict:
        if has_table(row.fqn):
            spark.sql(f"DROP TABLE IF EXISTS {row.fqn}")
            # result = create_status(
            #     scope=jp_scope,
            #     status_code=200,
            #     status_message=f"DROPPED: {row.fqn}",
            #     status_ctx=row,
            # )
            return result
            # else:
            #     result = create_status(
            #         scope=jp_scope,
            #         status_code=404,
            #         status_message=f"NOT_FOUND: {row.fqn}",
            #         status_ctx=row,
            #     )
            return result

    # async def drop_tables(df):
    #     ioresults = []
    #     ioresults.extend( await run_parallel(*[drop_table_delta(row) for row in df.collect()]))
    #     return ioresults

    results = await run_parallel(*[drop_table_delta(row) for row in df.collect()])

    # for result in map(lambda row: drop_table_delta(row), df.collect()):
    errors = []
    for result in results:
        if result["status_code"] != 404:
            # dropped or exception
            print(result)
            log_to_delta(result)
            if result["status_code"] >= 500:
                errors.append(result)
        elif DEBUG:
            print(result)

    if jp_action == "DROP":
        if errors and jp_stop_on_exception:
            raise Exception("errors occured in notebook")
        else:
            dbutils.notebook.exit(json.dumps(errors))  # empty or with errors
    else:
        jp_action = "CREATE"
        jp_actions = [jp_action]
        df = df.withColumn("action", F.lit(jp_action))
        display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SCHEMA if not exists
# MAGIC For this we create a unique list of schema names based on the work item list

# COMMAND ----------

if "CREATE" in jp_actions:

    async def create_schema(row):
        sql = f"CREATE SCHEMA IF NOT EXISTS {row.catalog_name}.{row.schema_name} WITH DBPROPERTIES (scope='{row.scope}')"
        spark.sql(sql)
        return (True, sql)

    errors = await run_parallel(
        *[
            create_schema(row)
            for row in df.select("catalog_name", "schema_name", "scope")
            .distinct()
            .collect()
        ]
    )
    display(errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## In CREATE mode only try to create tables that do not yet exist
# MAGIC Filter the list by checking if they already exist in target. This allows us to retry running this notebook and skip work items created in previous try

# COMMAND ----------


async def is_table_to_create(row):
    return (has_table(row.fqn), row.fqn)


tables_to_create = await run_parallel(*[is_table_to_create(row) for row in df.collect()])
# result is a list of tuples of (result, fqn)
tables_to_create = [t[1] for t in tables_to_create if not t[0]]

# tables_to_create = []
# for result, fqn in map(lambda row: is_table_to_create(row), df.collect()):
#     if not result:
#         tables_to_create.append(fqn)
print(tables_to_create[:3])

# COMMAND ----------

# MAGIC %md
# MAGIC Filter out the already created tables. If no tables are left we quit early

# COMMAND ----------

df = df.filter(F.col("fqn").isin(tables_to_create))

# if no work to do, quit
if df.count() == 0:
    dbutils.notebook.exit("{}")

# COMMAND ----------

df = df.withColumn("json", F.to_json(F.struct(*[F.col(c) for c in df.columns])))
work_items = [json.loads(value.json) for value in df.select("json").collect()]
print(len(work_items))

# COMMAND ----------

# MAGIC %md
# MAGIC - Initialize oracle connection
# MAGIC - Get count of each table
# MAGIC - Calculate partition size
# MAGIC - Sort tables (most records first)

# COMMAND ----------

# for each work item, get the count of the table and add it to the work item dict
# add the count of the table to the work item dict as the sql query
work_items = [
    wi
    | {
        "sql": f'SELECT COUNT(*) AS ROW_COUNT FROM {wi["schema_name_source"]}.{wi["table_name_source"]}'
    }
    for wi in work_items
]
display(work_items[:3])

# COMMAND ----------

db_conn_props: dict = get_connection_properties__by_key(jp_db_scope, p_db_key)

# create deep copy db_conn_props to db_dict
db_dict = copy.deepcopy(db_conn_props)
db_dict["pool_size_max"] = 20
db_dict["SIMULATE_LONG_RUNNING_QUERY"] = False
db_dict["VERBOSE"] = False
results_by_query_id = do_parallel_query(db_dict, work_items)


def get_count_and_calculate_partition_size(wi: dict, result: list) -> dict:

    range_size = int(result[0]["ROW_COUNT"])

    # get the count of the table
    if range_size == 0:
        logger.info(
            f"data source table empty: {wi['schema_name_source']}.{wi['table_name_source']}"
        )
    wi["row_count"] = range_size
    partition_bin_size = 100_000  # best same value as resultset size from db connection
    if range_size < partition_bin_size:
        wi["partition_count"] = 1 * wi["partition_multiplier"]
    else:
        # we want to have at least bin_size rows per partition with a max of 24 partitions
        wi["partition_count"] = (
            min(MAX_PARTITIONS, int(ceil(range_size / partition_bin_size)))
            * wi["partition_multiplier"]
        )

    del wi["sql"]
    return wi


work_items = [
    get_count_and_calculate_partition_size(wi, results_by_query_id[wi["__query_id__"]])
    for wi in work_items
]

pp.pp(work_items[:3])

# COMMAND ----------

# remove tables with 0 records (count = 0)
work_items = [work_item for work_item in work_items if work_item["row_count"] > 0]

if len(work_items) == 0:
    logger.info("after counting rows no more items were left - quiting")
    dbutils.notebook.exit("{}")


# sort workitems by count descending to get the biggest tables first
work_items = sorted(work_items, key=lambda wi: wi["row_count"], reverse=True)

# COMMAND ----------

if DEBUG:
    display(spark.createDataFrame(work_items))

# COMMAND ----------

results = []


def load_table(work_item) -> str:
    p_schema_name_source = work_item["schema_name_source"]
    p_table_name_source = work_item["table_name_source"]
    p_mode = work_item["mode"]
    # p_sql = work_item["query_sql"]
    logger.info(f"{p_schema_name_source}.{p_table_name_source} with mode {p_mode}")
    # Run the extract_table notebook
    result: str = dbutils.notebook.run(
        f"extract_table_{jp_run_version}",
        timeout_sec,
        {
            "p_work_json": json.dumps(work_item),
        },
    )
    logger.debug(json.dumps(work_item))
    return result


# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute the work items in parallel accross all worker nodes

# COMMAND ----------

start_time_load = time.time()
q = Queue()

thread_count = 0


def create_threads(threads_to_create):
    logger.info(f"create_threads: {threads_to_create}")
    global thread_count
    if threads_to_create > 0:
        for i in range(threads_to_create):
            t = Thread(target=run_tasks, args=(load_table, q))
            t.daemon = True
            thread_count += 1
            t.start()


def run_tasks(function, q):
    global thread_count
    while not q.empty():
        work_item: dict = q.get()
        try:
            result: str = function(work_item)
            result_dict = json.loads(result)
            work_item["job_id"] = result_dict.get("job_id", 0)
            work_item["column_name_pks"] = result_dict.get("column_name_pks", "")

            status_code = result_dict.get("status_code", -1)
            if status_code >= 300:
                logger.error(
                    f"ERROR - {result_dict.get('job_id', 0)}: {work_item.get('fqn', '')}, status_code: {result_dict.get('status_code', -1)}, time_duration: {result_dict.get('time_duration', -1)} sec ({result_dict.get('time_duration', -1)//60} min), status_message: {result_dict.get('status_message', '')}."
                )
            else:
                logger.info(
                    f"OK - {result_dict.get('job_id', 0)}: {work_item.get('fqn', '')}, status_code: {result_dict.get('status_code', -1)}, time_duration: {result_dict.get('time_duration', -1)} sec ({result_dict.get('time_duration', -1)//60} min), status_message: {result_dict.get('status_message', '')}."
                )

            sqls = []
            if work_item["mode"] == "overwrite":
                # first task that is creating the table shall also add some additional tags and PKs
                logging.debug('work_item["mode"] == "overwrite"')

                # add tags to the table for PII/confidential data
                fqn = work_item["fqn"]
                logging.debug(fqn)
                column_name_pks = work_item["column_name_pks"]
                logging.debug(column_name_pks)
                if work_item.get("pii", False):
                    sqls.append(f"ALTER TABLE {fqn} SET TAGS ('pii_table' = 'TRUE')")

                # create constraints for primary keys
                if column_name_pks:
                    sqls.append(f"ALTER TABLE {fqn} DROP PRIMARY KEY IF EXISTS CASCADE")
                    for column_name in column_name_pks.split(","):
                        sqls.append(
                            f"ALTER TABLE {fqn} ALTER COLUMN {column_name} SET NOT NULL"
                        )
                        sqls.append(
                            f"ALTER TABLE {fqn} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')"
                        )
                    table_name = work_item["table_name"]
                    column_name_pk = column_name_pks.split(",")[0]
                    sqls.append(
                        f"ALTER TABLE {fqn} ADD CONSTRAINT pk_{table_name}_{column_name_pk} PRIMARY KEY({column_name_pks})"
                    )

            for curr_sql in sqls:
                logging.debug(curr_sql)
                spark.sql(curr_sql)

            results.append(result)
            # if "children" in work_item:
            #     for child in work_item["children"]:
            #         q.put(create_work_item(child))

            #     # check if we have a thread for each child added
            #     child_count = len(work_item["children"])
            #     if child_count > thread_count:
            #         max_threads = (
            #             worker_count
            #             if worker_count < len(work_item["children"])
            #             else len(work_item["children"])
            #         )
            #         create_threads(max_threads - thread_count)

        except Exception as e:
            logger.error(
                f"called from run task {work_item.get('job_id', 0)}: {work_item.get('fqn', '')}: {e}"
            )
            if hasattr(e, "errmsg"):
                logger.error(e.errmsg)
            work_item["status_code"] = 500
            results.append(json.dumps(work_item))
        finally:
            q.task_done()

    # decrement the thread count beccause q is empty, thread is going to be killed
    thread_count -= 1
    # app_status["status_message"] = f"Thread finished. Remaining: {thread_count}"
    # app_status["status_code"] = 200
    # logger.info(app_status)
    # log_to_delta_table(app_status)


for work_item in work_items:
    q.put(work_item)

create_threads(worker_count)

q.join()

# COMMAND ----------

end_time = time.time()
time_duration = int(end_time - start_time)
print(f"duration notebook: {time_duration} seconds")
# print time_duration in format minutes:seconds
minutes = time_duration // 60
seconds = time_duration % 60
print(f"duration notebook: {minutes} minutes {seconds} seconds")
# load
time_duration = int(end_time - start_time_load)
print(f"duration notebook LOAD: {time_duration} seconds")
# print time_duration in format minutes:seconds
minutes = time_duration // 60
seconds = time_duration % 60
print(f"duration notebook LOAD: {minutes} minutes {seconds} seconds")


# COMMAND ----------

errors = []
for entry in results:
    try:
        entry = json.loads(entry)
        if entry.get("status_code", -1) >= 300:
            errors.append(entry)
            print(entry)
    except Exception as e:
        print(str(e))
        print(entry)

# COMMAND ----------

if errors:
    if jp_stop_on_exception:
        raise Exception("errors occured in notebook")
    else:
        dbutils.notebook.exit(json.dumps(errors))

# COMMAND ----------

dbutils.notebook.exit("{}")
