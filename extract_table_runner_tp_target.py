# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import json
import time
from helpers.status_helper import create_status
import concurrent.futures

# COMMAND ----------

table_names = [
    {"name": "STAGING.IOT_STG_LEM_NO_WORKING_DAYS", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_NO_WORKING_DAYS_TP", "pii": False},
    {"name": "STAGING.IOT_STG_DSR_DLR_DIST_LKP", "pii": False},
    {"name": "STAGING.IOT_STG_EMOT_LAST_DEREG", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_DLR_DIST_LKP", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_CUSTOMS_STATES", "pii": False},
    {"name": "STAGING.IOT_STG_EMOT_LOAD_RECEIVE", "pii": False},
    {"name": "STAGING.IOT_STG_EPD_LPG_CUSTOMIZATION", "pii": False},
    {"name": "STAGING.IOT_STG_EMOT_MLE_INVOICE", "pii": False},
    {"name": "STAGING.STG_VIN_LEM_NO_WORKING_DAYS", "pii": False},
    {"name": "STAGING.IOT_STG_EMOT_NSC_INVOICE", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_PROCESS_STATES", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_REPAIR_STATES", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_TRANSPORT_MEXICO", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_TRANSPORT_STATES", "pii": False},
    {"name": "STAGING.IOT_STG_LEM_VPR_SUMMARY", "pii": False},
    {"name": "STAGING_TEMP.STG_TMP_LEM_PDI_RDY_DATE", "pii": False},
    {"name": "STAGING_TEMP.STG_TMP_LEM_FIRST_TR_UNLOAD", "pii": False},
    {"name": "STAGING.STG_VIN_LEM", "pii": False},
    {"name": "STAGING.STG_VIN_WRTY", "pii": False},
    {"name": "STAGING_TEMP.T$_TTL_PPLN", "pii": False},
    {"name": "STAGING_TEMP.T$_IOT_DIM_CMPD", "pii": False},
    {"name": "STAGING_TEMP.T$_FIRST_DEMO_OREC", "pii": False},
    {"name": "STAGING.STG_VIN_EMOT", "pii": True},
    {"name": "STAGING_TEMP.V$_TEMP_200_ALL_SVC", "pii": False},
    {"name": "STAGING_TEMP.V$_TEMP_210_VHM_INFO", "pii": False},
    {"name": "STAGING.STG_VIN_DSR", "pii": False},
    {"name": "STAGING.STG_DIM_VIN", "pii": True},
]
p_scope = "ACC"
p_catalog_name_target = "impetus_target"

# COMMAND ----------

def get_schema(x):
    schema =  x["name"].split(".")[0]
    if schema == "STAGING":
        return "STG"
    elif schema == "LANDING_ZONE_LEMANS":
        return "LZ_LEM"
    elif schema == "STG":
        return "STG"
    else:
        raise Exception(f"Unkown schema: {x}")
get_table = lambda x: x["name"].split(".")[1]
extracts = [
    {"table": get_table(table_name), "schema": get_schema(table_name)}
    for table_name in table_names if table_name["pii"] == False
]
# extracts = [ get_schema_by_table(table_name) for table_name in table_names]
extracts

# COMMAND ----------

# from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

results = []

def do_task(extract):
    start_time = time.time()
    p_schema_name_source = extract["schema"]
    p_table_name_source = extract["table"]
    print(f"Extracting {p_schema_name_source}.{p_table_name_source}")
    # Run the extract_table notebook
    result = dbutils.notebook.run(
        "extract_table",
        0,
        {
            "p_scope": p_scope,
            "p_catalog_name_target": p_catalog_name_target,
            "p_schema_name_source": p_schema_name_source,
            "p_table_name_source": p_table_name_source,
        },
    )
    return result
    print(result)
    # parse string to json
    result = json.loads(result)
    if result["status_code"] >= 300:
        raise Exception(result["status_message"])
    end_time = time.time()
    time_duration = int(end_time - start_time)
    return f"OK: {p_schema_name_source}.{p_table_name_source} completed in {time_duration} seconds"

try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Submit the conversion tasks to the thread pool
        futures = []

        for extract in extracts:
            # Submit the conversion task to the thread pool
            future = executor.submit(do_task, extract=extract)
            futures.append(future)
        
        # Wait for all conversion tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                print(result)
                results.append(result)
            except Exception as e:
                print(f"An error occurred: with {result}\n{e}")

except Py4JJavaError as jex:
    print(str(jex.java_exception))
    print("TIMEDOUT" in str(jex.java_exception))
    end_time = time.time()
    time_duration = int(end_time - start_time)
    raise Exception(f"ERROR: TIMEDOUT: {time_duration} seconds") from jex

# except AnalysisException as ex:
# dbutils.notebook.exit(str(ex))
# except Exception as e:
# dbutils.notebook.exit(str(e))

# COMMAND ----------

dbutils.notebook.exit(create_status("OK: Notebook Completed"))
