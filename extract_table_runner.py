# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json
import os
import time

from helpers.db_helper import table_exists
from helpers.status_helper import create_status

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
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2024-01-01' AND CREATE_TS <= '2024-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2023-01-01' AND CREATE_TS <= '2023-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2022-01-01' AND CREATE_TS <= '2022-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2021-01-01' AND CREATE_TS <= '2021-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2020-01-01' AND CREATE_TS <= '2020-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2019-01-01' AND CREATE_TS <= '2019-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2018-01-01' AND CREATE_TS <= '2018-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2017-01-01' AND CREATE_TS <= '2017-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2016-01-01' AND CREATE_TS <= '2016-12-31'",
        'mode': 'append',
    },
    {
        'name': 'STG.STG_EMOT_BTNPPLNH',
        'pii': False,
        'query_type': 'query',
        'query_sql': "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2015-01-01' AND CREATE_TS <= '2015-12-31'",
        'mode': 'append',
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
]


p_scope = 'ACC'
p_db_key = 'DWH_BI1__100000' or 'DWH_BI1'
extract_pii = False
catalog_name_target = 'impetus_ref'
p_catalog_name_target = (
    catalog_name_target if not extract_pii else catalog_name_target + '_pii'
)
cpu_count = 6 if os.cpu_count() > 6 else os.cpu_count() - 1
timeout_sec = 3600

# COMMAND ----------


def get_schema_name_source(x):
    schema = x['name'].split('.')[0]
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

work_items = [
    {
        'pii': work_json['pii'],
        'timeout_sec': timeout_sec,
        'scope': p_scope,
        'catalog_name_source': '',
        'schema_name_source': get_schema_name_source(work_json),
        'table_name_source': get_table_name_source(work_json),
        'catalog_name_target': p_catalog_name_target,
        'schema_name_target': get_schema_name_source(work_json).lower().replace('$', '_'),
        'table_name_target': get_table_name_source(work_json).lower().replace('$', '_'),
        'db_key': p_db_key,
        'query_type': work_json.get('query_type', 'dbtable'),
        'query_sql': work_json.get('query_sql', None),
        'mode': work_json.get('mode', 'overwrite'),
    }
    for work_json in work_jsons
    if work_json['pii'] == extract_pii
]

# pre-filtering - only import tables we not already have
work_items = [
    work_item
    for work_item in work_items
    if not table_exists(
        work_item['catalog_name_target'],
        work_item['schema_name_target'],
        work_item['table_name_target'],
    )
    or work_item['mode'] == 'append'
]
work_items

# COMMAND ----------

# from pyspark.sql.utils import AnalysisException
# from py4j.protocol import Py4JJavaError

results = []


def do_task(work_item):
    start_time = time.time()
    p_schema_name_source = work_item['schema_name_source']
    p_table_name_source = work_item['table_name_source']
    print(f'Extracting {p_schema_name_source}.{p_table_name_source}')
    # Run the extract_table notebook
    result = dbutils.notebook.run(
        'extract_table',
        timeout_sec,
        {
            'p_work_json': json.dumps(work_item),
        },
    )
    return result
    print(result)
    # parse string to json
    result = json.loads(result)
    if result['status_code'] >= 300:
        raise Exception(result['status_message'])
    end_time = time.time()
    time_duration = int(end_time - start_time)
    return f'OK: {p_schema_name_source}.{p_table_name_source} completed in {time_duration} seconds'


# COMMAND ----------

for work_item in work_items:
    result = do_task(work_item)
    results.append(result)

# except Py4JJavaError as jex:
#     print(str(jex.java_exception))
#     print("TIMEDOUT" in str(jex.java_exception))
#     end_time = time.time()
#     time_duration = int(end_time - start_time)
#     raise Exception(f"ERROR: TIMEDOUT: {time_duration} seconds") from jex

# except AnalysisException as ex:
# dbutils.notebook.exit(str(ex))
# except Exception as e:
# dbutils.notebook.exit(str(e))

# COMMAND ----------

for entry in results:
    entry = json.loads(entry)
    import pprint as pp

    pp.pprint(entry)

# COMMAND ----------

dbutils.notebook.exit(create_status('OK: Notebook Completed'))
