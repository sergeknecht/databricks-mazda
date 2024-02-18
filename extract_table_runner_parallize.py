# Databricks notebook source
%md
# Data Extraction Runner Oracle to Delta Lake
## Can run in parallel in refresh (drop and re-create) or create mode (if not exists)

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import logging
import json
import time

# COMMAND ----------

# spark.sparkContext.setLogLevel("WARN")
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# logger.warning("This is a warning message")
# logger.error("This is an error message")
# logger.debug("About to do something")
# logger.info("This is an informational message")

# COMMAND ----------

# JOB PARAMETERS
jp_action = 'drop__create' or 'create' or 'drop'
jp_scope = 'ACC' or 'PRD' or 'TST' or 'DEV'
p_db_key = 'DWH_BI1__500000' or 'DWH_BI1__250000' or 'DWH_BI1__100000' or 'DWH_BI1'
extract_pii = False
jp_catalog_name_default = 'impetus'

# TODO: minus 1 ? because have not yet figured out how single nodes handle this number, and we want to avoid cpu starvation
cpu_count = sc.defaultParallelism # 6 if os.cpu_count() > 6 else os.cpu_count() - 1
print(sc.defaultParallelism)

timeout_sec = 3600

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# COMMAND ----------

work_jsons = [
    # {"name": "STG.STG_LEM_NO_WORKING_DAYS", "pii": False},
    # {"name": "STG.STG_LEM_COUNTRIES", "pii": False},
    # {"name": "STG.IOT_STG_DLR_DIST_INTERFACE", "pii": False},
    # {"name": "STG.IOT_DIM_DIST", "pii": False},
    # {"name": "STG.STG_DSR_DIST_CONVERSION", "pii": False},
    # {"name": "STG.STG_DSR_VEHICLE_MASTER", "pii": False},
    # {"name": "STG.STG_DSR_SVC_RECORDS", "pii": False},
    # {"name": "STG.STG_EMOT_BTNREGH", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_DISTRIBUTORS", "pii": False},
    # {"name": "STG.STG_LEM_DEALERS", "pii": False},
    # {"name": "STG.STG_LEM_DISTRIBUTORS", "pii": False},
    # {"name": "STG.STG_MUD_INTERFACE_CODE", "pii": False},
    # {"name": "STG.IOT_DIM_DLR_CAR_DELV_ADDRESS", "pii": False},
    # {"name": "STG.IOT_DIM_DLR", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_STATE_FLOW", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_STATE_VALUES", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_STATE_TYPES", "pii": False},
    # {"name": "STG_TMP.STG_TMP_COC_VINS", "pii": False},
    # {"name": "STG.STG_EMOT_BTNCDHDR", "pii": True},
    # {
    #     "name": "STG.STG_EMOT_BTNPPLNH",
    #     "pii": False,
    #     "mode": "drop",
    # },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2024-01-01' AND CREATE_TS <= '2024-12-31'",
        # "mode": "append",
    },
    # {"name": "STG.STG_EPD_CUSTCLASSIFICATION", "pii": False},
    # {"name": "STG.STG_EPD_CSTCLSSIFICATIONASSIGN", "pii": False},
    # {"name": "STG.STG_EPD_CUSTASSIGN", "pii": False},
    # {"name": "STG.STG_EPD_MODEL", "pii": False},
    # {"name": "STG.IOT_DIM_VEH_PRODUCT", "pii": False},
    # {"name": "STG.STG_EMOT_BTNIHDR", "pii": False},
    # {"name": "STG.STG_VIN_LEM", "pii": False},
    # {"name": "STG.STG_LEM_DAMAGE_CASES", "pii": False},
    # {"name": "STG.STG_LEM_VEH_CMPD_INFORMATION", "pii": False},
    # {"name": "STG.STG_LEM_COMPOUNDS", "pii": False},
    # {"name": "STG.STG_LEM_PARTNER_CODES", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_ROUTE_SEGMENTS", "pii": False},
    # {"name": "STG.STG_LEM_ROUTE_SEGMENTS", "pii": False},
    # {"name": "STG.STG_LEM_TRANSPORT_LINES", "pii": False},
    # {"name": "STG.STG_LEM_TRANSPORT_LINE_DEFS", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLES", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_ROUTES", "pii": False},
    # {"name": "STG.STG_LEM_ROUTES", "pii": False},
    # {"name": "STG.STG_LEM_ROUTE_DEFS", "pii": False},
    # {"name": "STG.STG_LEM_PROCESSING_TYPES", "pii": False},
    # {"name": "LZ_LEM.VPR_SUMMARY", "pii": False},
    # {"name": "STG.STG_LEM_VPR_SUMMARY", "pii": False},
    # {"name": "STG.STG_LEM_VPR_TYPES", "pii": False},
    # {"name": "STG.STG_LEM_OUTGOING_MESSAGES", "pii": False},
    # {"name": "STG.STG_LEM_TRANSPORT_MODES", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_ORIGINS", "pii": False},
    # {"name": "STG.STG_LEM_PORTS", "pii": False},
    # {"name": "STG.STG_LEM_ORIGINS", "pii": False},
    # {"name": "STG.STG_LEM_VOYAGES", "pii": False},
    # {"name": "STG.STG_LEM_VESSELS", "pii": False},
    # {"name": "STG.IOT_DIM_COUNTRY", "pii": False},
    # {"name": "STG.STG_LEM_PLANNING_PRIORITIES", "pii": False},
    # {"name": "STG.STG_LEM_INCO_TERMS", "pii": False},
    # {"name": "STG.STG_LEM_CURRENCIES", "pii": False},
    # {"name": "STG.IOT_DIM_CUR", "pii": False},
    # {"name": "STG.STG_LEM_LANGUAGES", "pii": False},
    # {"name": "STG.IOT_DIM_SUPPLY_CHAIN_REGION", "pii": False},
    # {"name": "STG.IOT_DIM_CMPD", "pii": False},
    # {"name": "STG.IOT_DIM_VIN", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_DESTINATIONS", "pii": False},
    # {"name": "STG.STG_LEM_DESTINATIONS", "pii": False},
    # {"name": "STG.STG_LEM_VEHICLE_DIST_PRICES", "pii": False},
    # {"name": "STG.STG_LEM_RTDAM_AUDIT", "pii": False},
    # {"name": "STG.STG_LEM_PLANTS", "pii": False},
    # {"name": "STG.IOT_DIM_PRTN", "pii": False},
    # {"name": "STG.STG_VEH_MASTER_BTV14010", "pii": False},
    # {"name": "STG.IOT_DIM_INTR_COLOR", "pii": False},
    # {"name": "STG.IOT_DIM_EXTR_COLOR", "pii": False},
    # {"name": "STG.STG_EMOT_BTNPLDEL", "pii": False},
    # {"name": "STG.STG_EMOT_BTNPPLN", "pii": False},
    # {"name": "STG.STG_EMOT_BTNORECH", "pii": False},
    # {"name": "STG.STG_EMOT_BTNOREC", "pii": False},
    # {"name": "STG.STG_SIEBEL_AGREEMENT_DATA", "pii": False},
    # {"name": "STG.STG_EMOT_BTNCRCAR", "pii": False},
    # {"name": "STG.STG_EMOT_BTNREGFR", "pii": False},
    # {"name": "STG.IOT_DIM_COC_TYPE_APPROVAL", "pii": False},
    # {"name": "STG.STG_EMOT_BTNREGSIV", "pii": False},
    # {"name": "STG.STG_EMOT_BTNREGEXT", "pii": False},
    # {"name": "STG.STG_EMOT_BTNCUSTCONTRACT", "pii": True},
    # {"name": "STG.STG_EMOT_BTNMUSER", "pii": True},
    # {"name": "STG.STG_EMOT_BTNCUSTOMER", "pii": False},
    # {"name": "STG.STG_EMOT_BTNREG", "pii": False},
    # {"name": "STG.STG_EMOT_BTNBPM", "pii": False},
    # {"name": "STG.STG_EMOT_BTNREGEXDEMO", "pii": False},
    # {"name": "STG.STG_EMOT_BTNCOCV", "pii": False},
    # {"name": "STG.STG_EMOT_BTNPPEXT", "pii": False},
    # {"name": "STG.STG_EMOT_BTNVBLFL", "pii": False},
    # {"name": "STG.IOT_DIM_VEH_PRODUCT_LOCAL", "pii": False},
    # {"name": "STG.IOT_STG_EMOT_BTNMPROP", "pii": False},
    # {"name": "STG.STG_EMOT_BTNDOC_EVENTS", "pii": False},
    # {"name": "STG.STG_TECH_DOCUMENT_STATUS", "pii": False},
    # {"name": "STG.STG_EMOT_BTNADDR", "pii": True},
    # {"name": "STG.STG_EMOT_BTNDOC_EVENTS_DETAIL", "pii": False},
    # {"name": "STG.STG_EMOT_BTNBOEVT", "pii": False},
    # {"name": "STG.STG_EMOT_BTNRPRTT", "pii": False},
    # {"name": "STG.STG_EMOT_BTNRPRT", "pii": False},
    # {"name": "STG.STG_DSR_SVC_REC_ITEMS", "pii": False},
    # {"name": "STG.STG_DSR_SVC_ITEMS", "pii": False},
    # {"name": "STG.STG_DSR_RSA_SARA_PROVIDERS", "pii": False},
    # {"name": "STG.STG_DSR_VEHICLE_WARRANTY", "pii": False},
    # {"name": "STG.STG_VIN_EMOT", "pii": True},
    # {"name": "STG.STG_VIN_WRTY", "pii": False},
    # {"name": "STG.STG_VIN_LEM_NO_WORKING_DAYS", "pii": False},
    # {"name": "STG.STG_LEM_LEAD_TIME_TARGETS", "pii": False},
    # {"name": "STG.STG_VIN_DSR", "pii": False},
    # {"name": "STG.STG_EMOT_BTNPROP_XREF_REL", "pii": False},
    # {"name": "STG.STG_EMOT_BTNPROP_XREF", "pii": False},
       {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2023-01-01' AND CREATE_TS <= '2023-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2022-01-01' AND CREATE_TS <= '2022-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2021-01-01' AND CREATE_TS <= '2021-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2020-01-01' AND CREATE_TS <= '2020-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2019-01-01' AND CREATE_TS <= '2019-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2018-01-01' AND CREATE_TS <= '2018-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2017-01-01' AND CREATE_TS <= '2017-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2016-01-01' AND CREATE_TS <= '2016-12-31'",
        "mode": "append",
    },
    {
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
        "query_type": "query",
        "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2015-01-01' AND CREATE_TS <= '2015-12-31'",
        "mode": "append",
    },
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_NO_WORKING_DAYS", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_NO_WORKING_DAYS_TP", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_DSR_DLR_DIST_LKP", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_LAST_DEREG", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_DLR_DIST_LKP", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_CUSTOMS_STATES", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_LOAD_RECEIVE", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_EPD_LPG_CUSTOMIZATION", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_MLE_INVOICE", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.STG_VIN_LEM_NO_WORKING_DAYS", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_NSC_INVOICE", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_PROCESS_STATES", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_REPAIR_STATES", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_TRANSPORT_MEXICO", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_TRANSPORT_STATES", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_VPR_SUMMARY", "pii": False},
    # {"catalog": "impetus_target", "name": "STG_TMP.STG_TMP_LEM_PDI_RDY_DATE", "pii": False},
    # {"catalog": "impetus_target", "name": "STG_TMP.STG_TMP_LEM_FIRST_TR_UNLOAD", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.STG_VIN_LEM", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.STG_VIN_WRTY", "pii": False},
    # {"catalog": "impetus_target", "name": "STG_TMP.T$_TTL_PPLN", "pii": False},
    # {"catalog": "impetus_target", "name": "STG_TMP.T$_IOT_DIM_CMPD", "pii": False},
    # {"catalog": "impetus_target", "name": "STG_TMP.T$_FIRST_DEMO_OREC", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.STG_VIN_EMOT", "pii": True},
    # {"catalog": "impetus_target", "name": "STG_TMP.V$_TEMP_200_ALL_SVC", "pii": False},
    # {"catalog": "impetus_target", "name": "STG_TMP.V$_TEMP_210_VHM_INFO", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.STG_VIN_DSR", "pii": False},
    # {"catalog": "impetus_target", "name": "STG.STG_DIM_VIN", "pii": True},
]


# COMMAND ----------

def get_schema_name_source(x):
    schema = x["name"].split(".")[0]
    if schema == 'STAGING':
        return 'STG'
    elif schema == 'LANDING_ZONE_LEMANS':
        return 'LZ_LEM'
    elif schema == 'STG':
        return 'STG'
    elif schema == 'STAGING_TEMP':
        return 'STG_TMP'
    elif schema in ('STG', 'LZ_LEM', 'STG_TMP', 'DWH'):
        return schema
    else:
        raise Exception(f'Unknown schema: {x}')

get_table_name_source = lambda x: x['name'].split('.')[1]

# def get_catalog_name_source(work_json):
#     catalog_name = work_json.get('catalog', 'impetus')
#     return x.get('catalog', 'impetus')

work_items = [
    {
        'action': jp_action,  # 'drop', 'create', 'drop__create'
        'pii': work_json['pii'], # True or False
        'timeout_sec': timeout_sec,
        'scope': jp_scope,
        'catalog_name_source': '',
        'schema_name_source': get_schema_name_source(work_json),
        'table_name_source': get_table_name_source(work_json),
        'catalog_name': f"{jp_scope.lower()}__{work_json.get('catalog_name', jp_catalog_name_default)}" if not extract_pii else f"{jp_scope.lower()}__{work_json.get('catalog_name', jp_catalog_name_default)}_pii",
        'schema_name': get_schema_name_source(work_json)
        .lower()
        .replace('$', '_'),
        'table_name': get_table_name_source(work_json)
        .lower()
        .replace('$', '_'),
        'db_key': p_db_key,
        'query_type': work_json.get('query_type', 'dbtable'),
        'query_sql': work_json.get('query_sql', None),
        'mode': work_json.get('mode', 'overwrite'),
    }
    for work_json in work_jsons
    if work_json['pii'] == extract_pii
]

work_items = [
    wi | {"fqn": f"{wi['catalog_name']}.{wi['schema_name']}.{wi['table_name']}"}
    for wi in work_items
]

work_items
# COMMAND ----------

results = []

def load_table(work_item):
    start_time = time.time()
    p_schema_name_source = work_item['schema_name_source']
    p_table_name_source = work_item['table_name_source']
    # print(f'Extracting {p_schema_name_source}.{p_table_name_source}')
    logger.info(f'Extracting {p_schema_name_source}.{p_table_name_source}')
    # Run the extract_table notebook
    result = dbutils.notebook.run(
        'extract_table_v240217',
        timeout_sec,
        {
            'p_work_json': json.dumps(work_item),
        },
    )
    return result
    print(result)
    # parse string to json
    result = json.loads(result)
    if result['status_code'] >= 500:
        raise Exception(result['status_message'])
    end_time = time.time()
    time_duration = int(end_time - start_time)
    return f'OK: {p_schema_name_source}.{p_table_name_source} completed in {time_duration} seconds'

# COMMAND ----------

from queue import Queue
from threading import Thread

q = Queue()
worker_count = cpu_count
errors = []

def run_tasks(function, q):
    while not q.empty():
        try:
            value = q.get()
            results.append(function(value))
        except Exception as e:
            fqn = value['fqn']
            # msg = f"ERROR: {fqn} failed with {str(e)}"
            logger.exception(e)
            errors[value] = e
            # print(msg) # log to logging
        finally:
            q.task_done()

for work_item in work_items:
    q.put(work_item)

start_time = time.time()

for i in range(worker_count):
    t = Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()

end_time = time.time()
execution_time = end_time - start_time

logger.info(f"Execution time: {execution_time} seconds")

if len(errors)  == 0:
    logger.info("All tasks completed successfully")
else:
    msg = f"Errors during tasks {list(errors.keys())} -> \n {str(errors)}"
    raise Exception(msg)

# COMMAND ----------

error_results = []
for entry in results:
    entry = json.loads(entry)
    if entry['status_code'] >= 300:
        error_results.append(entry)

    import pprint as pp

    pp.pprint(entry)

# COMMAND ----------

if error_results:
    pp.pprint(error_results)
    dbutils.notebook.exit(json.dumps(error_results))

# COMMAND ----------

dbutils.notebook.exit("[]")
