# Databricks notebook source
import os
import json
import time
from helpers.db_helper import table_exists
from helpers.status_helper import create_status
import concurrent.futures

# COMMAND ----------

table_names = [
    # {"name": "STAGING.STG_LEM_NO_WORKING_DAYS", "pii": False},
    # {"name": "STAGING.STG_LEM_COUNTRIES", "pii": False},
    # {"name": "STAGING.IOT_STG_DLR_DIST_INTERFACE", "pii": False},
    # {"name": "STAGING.IOT_DIM_DIST", "pii": False},
    # {"name": "STAGING.STG_DSR_DIST_CONVERSION", "pii": False},
    # {"name": "STAGING.STG_DSR_VEHICLE_MASTER", "pii": False},
    # {"name": "STAGING.STG_DSR_SVC_RECORDS", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNREGH", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_DISTRIBUTORS", "pii": False},
    # {"name": "STAGING.STG_LEM_DEALERS", "pii": False},
    # {"name": "STAGING.STG_LEM_DISTRIBUTORS", "pii": False},
    # {"name": "STAGING.STG_MUD_INTERFACE_CODE", "pii": False},
    # {"name": "STAGING.IOT_DIM_DLR_CAR_DELV_ADDRESS", "pii": False},
    # {"name": "STAGING.IOT_DIM_DLR", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_STATE_FLOW", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_STATE_VALUES", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_STATE_TYPES", "pii": False},
    {"name": "STAGING.STG_EMOT_BTNPPLNH", "pii": False},
    # {"name": "STAGING.STG_EPD_CUSTCLASSIFICATION", "pii": False},
    # {"name": "STAGING.STG_EPD_CSTCLSSIFICATIONASSIGN", "pii": False},
    # {"name": "STAGING.STG_EPD_CUSTASSIGN", "pii": False},
    # {"name": "STAGING.STG_EPD_MODEL", "pii": False},
    # {"name": "STAGING.IOT_DIM_VEH_PRODUCT", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNIHDR", "pii": False},
    # {"name": "STAGING.STG_VIN_LEM", "pii": False},
    # {"name": "STAGING.STG_LEM_DAMAGE_CASES", "pii": False},
    # {"name": "STAGING.STG_LEM_VEH_CMPD_INFORMATION", "pii": False},
    # {"name": "STAGING.STG_LEM_COMPOUNDS", "pii": False},
    # {"name": "STAGING.STG_LEM_PARTNER_CODES", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_ROUTE_SEGMENTS", "pii": False},
    # {"name": "STAGING.STG_LEM_ROUTE_SEGMENTS", "pii": False},
    # {"name": "STAGING.STG_LEM_TRANSPORT_LINES", "pii": False},
    # {"name": "STAGING.STG_LEM_TRANSPORT_LINE_DEFS", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLES", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_ROUTES", "pii": False},
    # {"name": "STAGING.STG_LEM_ROUTES", "pii": False},
    # {"name": "STAGING.STG_LEM_ROUTE_DEFS", "pii": False},
    # {"name": "STAGING.STG_LEM_PROCESSING_TYPES", "pii": False},
    # {"name": "LANDING_ZONE_LEMANS.VPR_SUMMARY", "pii": False},
    # {"name": "STAGING.STG_LEM_VPR_SUMMARY", "pii": False},
    # {"name": "STAGING.STG_LEM_VPR_TYPES", "pii": False},
    # {"name": "STG.STG_LEM_OUTGOING_MESSAGES", "pii": False},
    # {"name": "STAGING.STG_LEM_TRANSPORT_MODES", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_ORIGINS", "pii": False},
    # {"name": "STAGING.STG_LEM_PORTS", "pii": False},
    # {"name": "STAGING.STG_LEM_ORIGINS", "pii": False},
    # {"name": "STAGING.STG_LEM_VOYAGES", "pii": False},
    # {"name": "STAGING.STG_LEM_VESSELS", "pii": False},
    # {"name": "STAGING.IOT_DIM_COUNTRY", "pii": False},
    # {"name": "STAGING.STG_LEM_PLANNING_PRIORITIES", "pii": False},
    # {"name": "STAGING.STG_LEM_INCO_TERMS", "pii": False},
    # {"name": "STAGING.STG_LEM_CURRENCIES", "pii": False},
    # {"name": "STAGING.IOT_DIM_CUR", "pii": False},
    # {"name": "STAGING.STG_LEM_LANGUAGES", "pii": False},
    # {"name": "STAGING.IOT_DIM_SUPPLY_CHAIN_REGION", "pii": False},
    # {"name": "STAGING.IOT_DIM_CMPD", "pii": False},
    # {"name": "STAGING.IOT_DIM_VIN", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_DESTINATIONS", "pii": False},
    # {"name": "STAGING.STG_LEM_DESTINATIONS", "pii": False},
    # {"name": "STAGING.STG_LEM_VEHICLE_DIST_PRICES", "pii": False},
    # {"name": "STAGING.STG_LEM_RTDAM_AUDIT", "pii": False},
    # {"name": "STAGING.STG_LEM_PLANTS", "pii": False},
    # {"name": "STAGING.IOT_DIM_PRTN", "pii": False},
    # {"name": "STAGING.STG_VEH_MASTER_BTV14010", "pii": False},
    # {"name": "STAGING.IOT_DIM_INTR_COLOR", "pii": False},
    # {"name": "STAGING.IOT_DIM_EXTR_COLOR", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNPLDEL", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNPPLN", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNORECH", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNOREC", "pii": False},
    # {"name": "STAGING.STG_SIEBEL_AGREEMENT_DATA", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNCRCAR", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNREGFR", "pii": False},
    # {"name": "STAGING.IOT_DIM_COC_TYPE_APPROVAL", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNREGSIV", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNREGEXT", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNCUSTCONTRACT", "pii": True},
    # {"name": "STAGING.STG_EMOT_BTNMUSER", "pii": True},
    # {"name": "STAGING.STG_EMOT_BTNCUSTOMER", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNREG", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNBPM", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNREGEXDEMO", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNCOCV", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNPPEXT", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNVBLFL", "pii": False},
    # {"name": "STAGING.IOT_DIM_VEH_PRODUCT_LOCAL", "pii": False},
    # {"name": "STAGING.IOT_STG_EMOT_BTNMPROP", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNDOC_EVENTS", "pii": False},
    # {"name": "STAGING.STG_TECH_DOCUMENT_STATUS", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNADDR", "pii": True},
    # {"name": "STAGING.STG_EMOT_BTNDOC_EVENTS_DETAIL", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNBOEVT", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNRPRTT", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNRPRT", "pii": False},
    # {"name": "STAGING.STG_DSR_SVC_REC_ITEMS", "pii": False},
    # {"name": "STAGING.STG_DSR_SVC_ITEMS", "pii": False},
    # {"name": "STAGING.STG_DSR_RSA_SARA_PROVIDERS", "pii": False},
    # {"name": "STAGING.STG_DSR_VEHICLE_WARRANTY", "pii": False},
    # {"name": "STAGING.STG_VIN_EMOT", "pii": True},
    # {"name": "STAGING.STG_VIN_WRTY", "pii": False},
    # {"name": "STAGING.STG_VIN_LEM_NO_WORKING_DAYS", "pii": False},
    # {"name": "STAGING.STG_LEM_LEAD_TIME_TARGETS", "pii": False},
    # {"name": "STAGING.STG_VIN_DSR", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNPROP_XREF_REL", "pii": False},
    # {"name": "STAGING.STG_EMOT_BTNPROP_XREF", "pii": False},
]
p_scope = "ACC"
p_db_key = "DWH_BI1__100000" or "DWH_BI1"
extract_pii = False
p_catalog_name_target = "impetus_ref" if not extract_pii else "impetus_ref_pii"
cpu_count = 6 if os.cpu_count() > 6 else os.cpu_count() -1
timeout_sec = 3600

# COMMAND ----------

def get_schema(x):
    schema =  x["name"].split(".")[0]
    if schema == "STAGING":
        return "STG"
    elif schema == "LANDING_ZONE_LEMANS":
        return "LZ_LEM"
    elif schema == "STG":
        return "STG"
    elif schema == "STAGING_TEMP":
        return "STG_TMP"
    else:
        raise Exception(f"Unkown schema: {x}")
get_table = lambda x: x["name"].split(".")[1]
extracts = [
    {"table": get_table(table_name), "schema": get_schema(table_name)}
    for table_name in table_names if table_name["pii"] == extract_pii
]
extracts = [
    extract
    for extract in extracts if not table_exists(p_catalog_name_target, extract["schema"].lower(), extract["table"].lower().replace("$", "_"))
]
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
        timeout_sec,
        {
            "p_scope": p_scope,
            "p_catalog_name_target": p_catalog_name_target,
            "p_schema_name_source": p_schema_name_source,
            "p_table_name_source": p_table_name_source,
            "p_db_key": p_db_key
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count) as executor:
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

import pprint as pp
pp.pprint(results)

# COMMAND ----------

dbutils.notebook.exit(create_status("OK: Notebook Completed"))
