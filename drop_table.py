# Databricks notebook source
# MAGIC %md
# MAGIC # Data Extraction Runner Oracle to Delta Lake
# MAGIC ## Can run in parallel in refresh (drop and re-create) or create mode (if not exists)

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2

# COMMAND ----------

import logging
import datetime
import json
import time

from helpers.app_helper import init
from helpers.logger_helper import log_to_delta_table
from helpers.status_helper import create_status
from helpers.db_helper_delta import get_table_exists
import pyspark.sql.functions as F

# COMMAND ----------

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# COMMAND ----------

dbutils.widgets.text(
    "jp_action", "drop", label="Job action: drop or create or drop__create"
)
dbutils.widgets.dropdown(
    "jp_stop_on_exception",
    "FALSE",
    ["TRUE", "FALSE"],
    label="raise exception on data error",
)

# COMMAND ----------

jp_action: str = dbutils.widgets.get("jp_action")
jp_stop_on_exception: bool = dbutils.widgets.get("jp_stop_on_exception").upper() == "TRUE"
jp_action + "," + str(jp_stop_on_exception)

# COMMAND ----------

# JOB PARAMETERS
# jp_action = "create" or "drop__create" or "create" or "drop__create" or "drop"
jp_actions = jp_action.split("__")
jp_scope = "DEV" or "PRD" or "TST" or "DEV"  # where to write the data
jp_db_scope = "ACC"  # where to read the data
jp_run_version = "v240304"  # version of the job
p_db_key = "DWH_BI1__100000" or "DWH_BI1" or "DWH_BI1__500000" or "DWH_BI1__250000"
run_ts = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
run_name = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)

# TODO: minus 1 ? because have not yet figured out how single nodes handle this number, and we want to avoid cpu starvation
cpu_count = max(
    8, int(sc.defaultParallelism * 0.85)
)  # 40 workers accross all nodes, but partitioning will create more tasks
worker_count = 5  # workers accross all nodes, but partitioning will create more tasks

if jp_action == "drop":
    worker_count = cpu_count * 5

print(cpu_count, worker_count)

timeout_sec = 5400  # 1:30 hours

start_time = time.time()


# COMMAND ----------

with open("./config/work_items__impetus_poc.json") as f:
    work_jsons = json.load(f)

print(len(work_jsons))

# COMMAND ----------

app_status = init(jp_scope)
display(app_status)

# COMMAND ----------

for wi in work_jsons:
    assert "pii" in wi, f"pii not found in {wi}"
    assert "catalog" in wi, f"catalog not found in {wi}"
    assert "name" in wi, f"name not found in {wi}"


def get_schema_name_source(x):
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


def create_work_item(wi):
    wi = {
        "action": jp_action,
        "pii": wi["pii"],
        "timeout_sec": timeout_sec,
        "scope": jp_scope,
        "catalog_name_source": "",
        "schema_name_source": get_schema_name_source(wi),
        "table_name_source": get_table_name_source(wi),
        "catalog_name": (
            f"{jp_scope.lower()}__{wi['catalog']}"
            if not wi["pii"]
            else f"{jp_scope.lower()}__{wi['catalog']}_pii"
        ),
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
        "partition_count": wi.get("partition_count", cpu_count),
    }
    wi["fqn"] = f"{wi['catalog_name']}.{wi['schema_name']}.{wi['table_name']}"
    return wi


work_items = [create_work_item(wi) for wi in work_jsons]

print(len(work_items))

# COMMAND ----------

work_items

# COMMAND ----------

df = spark.createDataFrame(work_items)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP tables if exist

# COMMAND ----------

if "drop" in jp_actions:
    def drop_table_delta(row):
        if get_table_exists(row.fqn):
            spark.sql(f"DROP TABLE IF EXISTS {row.fqn}")
            result = create_status(
                scope=jp_scope,
                status_code=200,
                status_message=f"DROPPED: {row.fqn}",
                status_ctx=row
            )
            return result
        else:
            result = create_status(
                scope=jp_scope,
                status_code=404,
                status_message=f"NOT_FOUND: {row.fqn}",
                status_ctx=row
            )
            return result

    errors = []
    for result in map(lambda row: drop_table_delta(row), df.collect()):
        if result["status_code"] != 404:
            # dropped or exception
            print(result)
            log_to_delta_table(result)
            if result["status_code"] >= 500:
                errors.append(result)

    if jp_action == "drop":
        if errors and jp_stop_on_exception:
            raise Exception("errors occured in notebook")
        else:
            dbutils.notebook.exit(json.dumps(errors)) # empty or with errors
    else:
        jp_action = "create"
        jp_actions = [jp_action]
        df = df.withColumn("action", F.lit(jp_action))
        display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SCHEMA if not exists

# COMMAND ----------

if "create" in jp_actions:
    def create_schema(row):
        sql = f"CREATE SCHEMA IF NOT EXISTS {row.catalog_name}.{row.schema_name}"
        spark.sql(sql)
        return (True, sql)

    errors = [result for result in  map(lambda row: create_schema(row), df.select("catalog_name", 'schema_name').distinct().collect())]
    display(errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## In CREATE mode only try to create tables that do not yet exist
# MAGIC ### Filter the list

# COMMAND ----------

def is_table_to_create(row):
    return (get_table_exists(row.fqn), row.fqn )       

create_list = []
for result in map(lambda row: is_table_to_create(row), df.collect()):
    if not result[0]:
        create_list.append(result[1])
print(create_list)

# COMMAND ----------

df.filter(lambda: row : get_table_exists(row)).show()

# COMMAND ----------

results = []


def load_table(work_item) -> str:
    p_schema_name_source = work_item["schema_name_source"]
    p_table_name_source = work_item["table_name_source"]
    p_action = work_item["action"]
    p_mode = work_item.get("mode", "overwrite")
    p_sql = work_item.get("query_sql", "")
    logger.info(
        f"{p_action} {p_schema_name_source}.{p_table_name_source} with mode {p_mode} with sql {p_sql}"
    )
    # Run the extract_table notebook
    result: str = dbutils.notebook.run(
        f"extract_table_{jp_run_version}",
        timeout_sec,
        {
            "p_work_json": json.dumps(work_item),
        },
    )
    return result


# COMMAND ----------

from queue import Queue
from threading import Thread

q = Queue()

thread_count = 0


def create_threads(threads_to_create):
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

            logger.info(
                f"completed {result_dict.get('job_id', 0)}: {work_item.get('fqn', '')}, status_code: {result_dict.get('status_code', -1)}, time_duration: {result_dict.get('time_duration', -1)} sec ({result_dict.get('time_duration', -1)//60} min), status_message: {result_dict.get('status_message', '')}."
            )

            results.append(result)
            if work_item["children"]:
                for child in work_item["children"]:
                    q.put(create_work_item(child))

                # check if we have a thread for each child added
                child_count = len(work_item["children"])
                if child_count > thread_count:
                    max_threads = (
                        worker_count
                        if worker_count < len(work_item["children"])
                        else len(work_item["children"])
                    )
                    create_threads(max_threads - thread_count)

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
    app_status["status_message"] = f"Thread finished. Remaining threads: {thread_count}"
    app_status["status_code"] = 200
    logger.info(app_status)
    log_to_delta_table(app_status)


for work_item in work_items:
    q.put(work_item)

create_threads(worker_count)

q.join()

# COMMAND ----------

end_time = time.time()
time_duration = int(end_time - start_time)
print(f"duration notebook seconds: {time_duration}")

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
