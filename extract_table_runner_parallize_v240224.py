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

# COMMAND ----------

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# COMMAND ----------

# JOB PARAMETERS
jp_action = "create" or "drop__create" or "create" or "drop__create" or "drop"
jp_actions = jp_action.split("__")
jp_scope = "ACC" or "PRD" or "TST" or "DEV"  # where to write the data
jp_db_scope = "ACC"  # where to read the data
jp_run_version = "v240224"  # version of the job
p_db_key = "DWH_BI1__250000" or "DWH_BI1__100000" or "DWH_BI1" or "DWH_BI1__500000"
run_ts = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
run_name = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)

# TODO: minus 1 ? because have not yet figured out how single nodes handle this number, and we want to avoid cpu starvation
cpu_count = max(8, int(sc.defaultParallelism * 0.85))  # 40 workers accross all nodes, but partitioning will create more tasks
worker_count = 5  # 5 workers accross all nodes, but partitioning will create more tasks

if jp_action == "drop":
    worker_count = cpu_count * 2

print(cpu_count, worker_count)

timeout_sec = 5400  # 1:30 hours

start_time = time.time()

# COMMAND ----------

result = init(jp_scope)
display(result)

# COMMAND ----------

# from helpers.logger_helper import CATALOG, SCHEMA, TABLE_APPLICATION, FQN
# # df=spark.createDataFrame([result])
# # df.write.format("delta").partitionBy("log_dt").mode("append").option("mergeSchema", "true").saveAsTable(FQN.format(scope=jp_scope))

# # test what will happen when target does not exist and we have no data (= no schema)

# from pyspark.errors import PySparkException
# try:
#     df=spark.createDataFrame([])
#     df.write.format("delta").partitionBy("log_dt").mode("append").option("mergeSchema", "true").saveAsTable(FQN.format(scope=jp_scope))
# except PySparkException as e:
#     print("we caught PySparkException")
#     logger.error(e.getErrorClass())
#     # logger.exception(e)
#     if e.getErrorClass() == "CANNOT_INFER_EMPTY_SCHEMA":
#         logger.warning("CANNOT_INFER_EMPTY_SCHEMA: DataFrame empty")
# except Exception as e:
#     print("we caught Exception")

# COMMAND ----------

work_jsons = [
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_NO_WORKING_DAYS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_COUNTRIES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_STG_DLR_DIST_INTERFACE", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_DIST", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_DSR_DIST_CONVERSION", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_DSR_VEHICLE_MASTER", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_DSR_SVC_RECORDS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNREGH", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_DISTRIBUTORS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_DEALERS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_DISTRIBUTORS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_MUD_INTERFACE_CODE", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_DLR_CAR_DELV_ADDRESS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_DLR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_STATE_FLOW", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_STATE_VALUES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_STATE_TYPES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG_TMP.STG_TMP_COC_VINS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNCDHDR", "pii": True},
    {
        "catalog": "impetus_poc",
        "name": "STG.STG_EMOT_BTNPPLNH",
        "pii": False,
    },
    # {
    #     "catalog": "impetus_poc",
    #     "name": "STG.STG_EMOT_BTNPPLNH",
    #     "pii": False,
    #     "mode": "drop",
    #     "children": [
    #         {
    #             "catalog": "impetus_poc",
    #             "name": "STG.STG_EMOT_BTNPPLNH",
    #             "pii": False,
    #             "query_type": "query",
    #             "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2024-01-01' AND CREATE_TS <= '2024-12-31'",
    #             "mode": "overwrite",
    #             "children": [
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2023-01-01' AND CREATE_TS <= '2023-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2022-01-01' AND CREATE_TS <= '2022-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2021-01-01' AND CREATE_TS <= '2021-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2020-01-01' AND CREATE_TS <= '2020-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2019-01-01' AND CREATE_TS <= '2019-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2018-01-01' AND CREATE_TS <= '2018-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2017-01-01' AND CREATE_TS <= '2017-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2016-01-01' AND CREATE_TS <= '2016-12-31'",
    #                     "mode": "append",
    #                 },
    #                 {
    #                     "catalog": "impetus_poc",
    #                     "name": "STG.STG_EMOT_BTNPPLNH",
    #                     "pii": False,
    #                     "query_type": "query",
    #                     "query_sql": "select * from STG.STG_EMOT_BTNPPLNH WHERE CREATE_TS >= '2015-01-01' AND CREATE_TS <= '2015-12-31'",
    #                     "mode": "append",
    #                 },
    #             ],
    #         }
    #     ],
    # },
    {"catalog": "impetus_poc", "name": "STG.STG_EPD_CUSTCLASSIFICATION", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EPD_CSTCLSSIFICATIONASSIGN", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EPD_CUSTASSIGN", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EPD_MODEL", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_VEH_PRODUCT", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNIHDR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_VIN_LEM", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_DAMAGE_CASES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEH_CMPD_INFORMATION", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_COMPOUNDS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_PARTNER_CODES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_ROUTE_SEGMENTS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_ROUTE_SEGMENTS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_TRANSPORT_LINES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_TRANSPORT_LINE_DEFS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_OUTGOING_MESSAGES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_ROUTES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_ROUTES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_ROUTE_DEFS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_PROCESSING_TYPES", "pii": False},
    {"catalog": "impetus_poc", "name": "LZ_LEM.VPR_SUMMARY", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VPR_SUMMARY", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VPR_TYPES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_OUTGOING_MESSAGES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_TRANSPORT_MODES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_ORIGINS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_PORTS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_ORIGINS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VOYAGES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VESSELS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_COUNTRY", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_PLANNING_PRIORITIES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_INCO_TERMS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_CURRENCIES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_CUR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_LANGUAGES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_SUPPLY_CHAIN_REGION", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_CMPD", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_VIN", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_DESTINATIONS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_DESTINATIONS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_VEHICLE_DIST_PRICES", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_RTDAM_AUDIT", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_PLANTS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_PRTN", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_VEH_MASTER_BTV14010", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_INTR_COLOR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_EXTR_COLOR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNPLDEL", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNPPLN", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNORECH", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNOREC", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_SIEBEL_AGREEMENT_DATA", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNCRCAR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNREGFR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_COC_TYPE_APPROVAL", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNREGSIV", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNREGEXT", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNCUSTCONTRACT", "pii": True},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNMUSER", "pii": True},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNCUSTOMER", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNREG", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNBPM", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNREGEXDEMO", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNCOCV", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNPPEXT", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNVBLFL", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_DIM_VEH_PRODUCT_LOCAL", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.IOT_STG_EMOT_BTNMPROP", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNDOC_EVENTS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_TECH_DOCUMENT_STATUS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNADDR", "pii": True},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNDOC_EVENTS_DETAIL", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNBOEVT", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNRPRTT", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNRPRT", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_DSR_SVC_REC_ITEMS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_DSR_SVC_ITEMS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_DSR_RSA_SARA_PROVIDERS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_DSR_VEHICLE_WARRANTY", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_VIN_EMOT", "pii": True},
    {"catalog": "impetus_poc", "name": "STG.STG_VIN_WRTY", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_VIN_LEM_NO_WORKING_DAYS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_LEM_LEAD_TIME_TARGETS", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_VIN_DSR", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNPROP_XREF_REL", "pii": False},
    {"catalog": "impetus_poc", "name": "STG.STG_EMOT_BTNPROP_XREF", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_NO_WORKING_DAYS", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_NO_WORKING_DAYS_TP", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_DSR_DLR_DIST_LKP", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_LAST_DEREG", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_DLR_DIST_LKP", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_CUSTOMS_STATES", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_LOAD_RECEIVE", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_EPD_LPG_CUSTOMIZATION", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_MLE_INVOICE", "pii": False},
    {"catalog": "impetus_target", "name": "STG.STG_VIN_LEM_NO_WORKING_DAYS", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_EMOT_NSC_INVOICE", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_PROCESS_STATES", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_REPAIR_STATES", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_TRANSPORT_MEXICO", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_TRANSPORT_STATES", "pii": False},
    {"catalog": "impetus_target", "name": "STG.IOT_STG_LEM_VPR_SUMMARY", "pii": False},
    {"catalog": "impetus_target", "name": "STG_TMP.STG_TMP_LEM_PDI_RDY_DATE", "pii": False},
    {"catalog": "impetus_target", "name": "STG_TMP.STG_TMP_LEM_FIRST_TR_UNLOAD", "pii": False},
    {"catalog": "impetus_target", "name": "STG.STG_VIN_LEM", "pii": False},
    {"catalog": "impetus_target", "name": "STG_TMP.T$_TTL_PPLN", "pii": False},
    {"catalog": "impetus_target", "name": "STG_TMP.T$_IOT_DIM_CMPD", "pii": False},
    {"catalog": "impetus_target", "name": "STG_TMP.T$_FIRST_DEMO_OREC", "pii": False},
    {"catalog": "impetus_target", "name": "STG.STG_VIN_EMOT", "pii": True},
    {"catalog": "impetus_target", "name": "STG_TMP.V$_TEMP_200_ALL_SVC", "pii": False},
    {"catalog": "impetus_target", "name": "STG_TMP.V$_TEMP_210_VHM_INFO", "pii": False},
    {
        "catalog": "impetus_target",
        "name": "STG.STG_DIM_VIN",
        "pii": True,
        # "db_key": "DWH_BI1__500000",
    },
]

# COMMAND ----------

work_jsons_source = [
    {"catalog": "impetus_src", "name": "LZ_BILL.TBCBI_CODE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BILL.TBCBI_ITEM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BILL.TBCBI_NONSTDVATCD", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BILL.TBMJY_PAROKEY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.BIFILEUPLOAD_VERSIONDESC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.CAUSE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.CLIENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.MARKET", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.MARKETDISTRIBUTORCOUNTRY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.MARKETMAPPING", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.MARKETSTRUCTURE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.MARKETTYPE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.PICK_PACK_CHECK_RESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.PICK_PACK_LOCATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.PICK_PACK_STATUS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_BIUT.WORKINGDAY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_COC.TBCOC_PRODUCT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_COC.TBCOC_REGEXCHG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_COC.TBCOC_REGEXCODE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_COC.TBCOC_REGEXFIELD", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_COC.TBCOC_STATUS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_COC.TBCOC_VALUE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_CTS2.APPLICATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_CTS2.KEY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_CTS2.LABEL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_CTS2.LANGUAGE_LOCALE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.ACCESS_LOG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.CHECKLIST", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.CHECK_ATTACHMENT_REF", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.CHECK_LOCATION_RESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.CHECK_REMINDER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.CHECK_RESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.CHECK_RESULT_DETAIL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.DEFECT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.DSB_COUNTRY_PARAMS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.DSB_DIST_NSC_CONVERSION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.EXTERNAL_CONTRACTS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.EXT_SVC_PLANS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.EXT_SVC_PLAN_ITEMS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.MAP_UPDATES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.MV_RPTBONUS_DETAIL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.MV_RPTBONUS_DETAIL_ROLL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.MV_RPTBONUS_SUMMARY_DLR", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_DSR.MV_RPTBONUS_SUMMARY_DLR_ROLL",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_DSR.MV_RPTBONUS_SUMMARY_NSC", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_DSR.MV_RPTBONUS_SUMMARY_NSC_ROLL",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_DSR.MV_VEHICLE_WARRANTY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.NSC_DLR_CONVERSIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.OERSA_CONTRACTS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.OEW_CONTRACTS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.OFFERLI_ATTREF_RELATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.OFFER_ATTREF_RELATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.RSA_SARA_PROVIDERS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.SVC_RECORDS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.SVC_REC_ITEMS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.SVC_REC_ITEMS_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.VEHICLE_MASTER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.VIN_LOCATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.WO_TRANSMISSIONS_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_INVOICES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_INVOICES_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_INVOICE_ITEMS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_INVOICE_ITEMS_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_ORDERS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_ORDERS_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_ORDER_ITEMS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_ORDER_ITEMS_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_DSR.W_ORDER_SVC_ITEMS_CURR", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_EBONUS.MV_DWH_CALCULATION_CURRENT",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_ECLAIMS.CLAIM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ECLAIMS.CLAIM_DECISION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ECLAIMS.EXTERNAL_ADVICE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNBOEVT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNBPM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNCOCV", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNCRCAR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNCRINFH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNCUSTCONTRACT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNCUSTOMER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNDEAL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNDEAL_AUDIT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNDOC_EVENTS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNDOC_EVENTS_DETAIL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNIHDR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNMUSER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNORDFORM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNORDRQ", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNOREC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNORECH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNORPND", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPCST", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPDISC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPDSC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPGPC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPLNMV", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPPLN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPPLNH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPROCRESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPROP_XREF", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNPROP_XREF_REL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNREG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNREGEXDEMO", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNREGEXT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNREGFR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNREGH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNREGSIV", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNRPRTT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EMOT.BTNVINH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBBAS_DISASSIGN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBBAS_DISTRIBUT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBBAS_LANASSIGN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBBAS_LANGUAGE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_BASECODE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_COLASSIGN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_COLOUR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_CUSTASSIGN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_CUSTCLASSIFICATION", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_EPD.TBEPD_CUSTCLASSIFICATIONASSIGN",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_ELEASSIGN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_ELEMENT", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_EPD.TBEPD_GVSCARLINEMODELASSIGN",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_MODEL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_MSCASSIGN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_PLUFLAG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_PLUFLAGHISTORY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPD.TBEPD_SERIES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.ATTACHMENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.CHECKSHEET_TYPE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.CODETYPE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.IGNITIONKEY_LOG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.IMMOBILIZER_LOG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.PQI", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.PQI_CVC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.PQI_DTC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.PQI_HISTORY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.REQMASTER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.REQMASTER_HISTORY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.REQMASTER_PART", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.REQMASTER_REQUESTOR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.TC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.TC_HISTORY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EPSS.TC_MODEL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EQT.BI_OFFER_EVENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EQT.BI_OFFER_EVENT_ACCESSORY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EQT.BI_OFFER_EVENT_USED_CAR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EQT.BI_OFFER_EVENT_VEH_DISC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_EQT.BI_SALES_CONTRACT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGADDRESSES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGCOMPANIES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGEMPLOYMENT_STATUS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGFUNCTIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGJOBROLES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGUSERFUNCTIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGUSERS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_ETAS.BTGUSER_DETAILS", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_CONLEAD_DEL_EXP", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_CONTACTS_CURR", "pii": False},
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACTS_DEL_EXP",
    #     "pii": False,
    # },
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_CONTACTS_EXPORT", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_CONTACTS_LIST", "pii": False},
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACTS_RTBD_DEL_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_EMAILS_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_EMAIL_VEH_EXP",
    #     "pii": False,
    # },
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_CONTACT_FILTER", "pii": False},
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_LEADS_CURR",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_LEADS_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_ORDERS_CURR",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_ORDERS_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_ORDERS_LIST",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_ORD_DEL_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_TCMR_CURR",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_TCMR_DEL_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_TCMR_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_TSC_CURR",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_TSC_DEL_EXP",
    #     "pii": False,
    # },
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_CONTACT_TSC_EXP", "pii": False},
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_TSC_UPD_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_VEHICLES_CURR",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_VEHICLES_EXP",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_VEHICLES_LIST",
    #     "pii": False,
    # },
    # {
    #     "catalog": "impetus_src",
    #     "name": "LZ_EXTRACT.ELOQUA_CONTACT_VEH_DEL_EXP",
    #     "pii": False,
    # },
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_CONVEH_DEL_EXP", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.ELOQUA_MARKETS", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_EXTRACT.MMUK_MMPSREG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FILE.CEMI", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FILE.CIP_CUSTOMER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FILE.CIP_MASTER_CUSTOMER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FILE.RDC_CANCELLATIONS_DAILY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FILE.RDC_ORDER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FILE.RDC_ORDER_DAILY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FILE.RDC_PICKUP_DAILY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.ACCESSORIES", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_FMS.ACCOUNTINGCATEGORY_MAPPINGS",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_FMS.CARS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.CARSTATES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.CAR_CALCULATIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.CAR_LICENSEPLATE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.CAR_TYPES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.CURRENCIES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.DISTANCES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.DISTANCEUNITS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.INVOICES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.LICENSEPLATES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.MODELS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.NETAMOUNTS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.ONLINE_STATES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.ORDERS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.PRECARORDERS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.PRICES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.USAGES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_FMS.USAGE_TYPES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.DWR_PAGE_VIEWS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.MONTHLY_WEB_REPORT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.MWR_CAMPAIGN_GOALS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.MWR_SESSIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.MWR_SOURCE_MEDIUM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.MWR_URL_CATEGORY_PAGE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.MWR_URL_LANDING_PAGE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_GA.WEBSITE_TRACKING", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.CATEGORY_COMPOSING", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.CATEGORY_DESCRIPTION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.E_COMPONENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.E_COMPONENTTYPE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.E_COMPONENT_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.E_DESCRIPTION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.E_DESCRIPTION_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.LEARNINGFORM_D", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_IMC.PARTICIPANT_PREBOOKING_CURR",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_IMC.PERSON", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_IMC.PERSON_COMPONENT_ASSIGNMENT",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_IMC.PERSON_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.PERSON_CUSTOM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.PORTFOLIO", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.PORTFOLIO_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.R_LOCATION_STRUCTURE_D", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.TOPIC_COMPOSING", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.TOPIC_COMPOSING_IDS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.TOPIC_DESCRIPTION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_IMC.TOPIC_ROOT_D", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_DEALER_SETTINGS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_MATCH_EXPLANATIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_MODEL_GROUP", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_MODEL_GROUP_CARLINE", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_INC.DMM_MODEL_GROUP_TARGET_RANGE",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_POS_DLR_STTNGS_MGR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_RESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_SANCTIONING", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_SANCTIONING_RESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_TDEFI", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_VIN_DATA", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.DMM_VIN_RESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBDFP_VINPAYMENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_CAMPAIGN_CAR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_COND", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_CPNTYPE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_EMOTIVMIRROR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_EMOTIVMIRROR_INVOICE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_FINZINF", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_MATCH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_RESULT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_TDEFI", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_INC.TBINC_VINPAYMENT", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.CA_ABSENCE_REQUEST", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.CO_EMPLOYEE", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.CO_EMPLOYEE_SYNC", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.CUST_MAZDA_ABSENCES", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.CUST_MAZDA_EMPLOYEES", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.CUST_MAZDA_PUNCHES", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.EMPLOYEE", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.EMPLOYEE_SYNC", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.TM_EXCDEF", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.TM_EXCEPTION", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.TM_LINK", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.TM_PUNCH", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.TM_SCHEDULE", "pii": False},
    # {"catalog": "impetus_src", "name": "LZ_KRONOS.TM_TEMPLATE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.COST_DETAILS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.COST_DETAILS_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.DDN_RTDAM_AUDIT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.DDN_VEHICLE_DISTRIBUTORS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.DDN_VEHICLE_STATE_FLOW", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.MPI_INVOICES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.RTDAM_AUDIT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLE_ACCESSORIES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLE_DESTINATIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLE_DISTRIBUTORS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLE_ORIGINS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLE_PROCESS_STATUSES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLE_ROUTE_SEGMENTS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VEHICLE_STATE_FLOW", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VPR_SENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_LEM.VPR_SUMMARY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_DEALER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_DEALER_CODE_HISTORY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_DEALER_GROUP", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_DEALER_SERVICE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_DISTRIBUTOR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_INTERFACE_CODE", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_MUD.BI_PARTS_DISTRIBUTOR_SERVICE",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_RELATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_SALESTARGET", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUD.BI_SALESTARGETPROPOSAL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_MUM.TUSER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.ALL_PART_FLAGS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.CLASSIFICATION_HIST", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.MV_CLASSIFICATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_CAMPAIGN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_CIF_ANTWERP", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_DEALER_NET", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_FOB_BRUSSELS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_FOB_JAPAN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_MASTER_RETAIL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_PART_FLAGS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_SIMULATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_NGEPS.V_TRANSFER_CIF", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_OSB.CHANGE_LOG_FILES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_OSB.EVENT_DETAILS_FILES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_OSB.LOGIN_FILES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_OTM.IE_SHIPMENTSTATUS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_OTM.SHIPMENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_OTM.XXSPE_SHIPMENTSTATUS_V", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSBDYTP", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSCARLN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSCLAIM_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSCLDLR_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSCLLOG_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSCTCRL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDLDCH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDLRDT_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDLRGN_MUD", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDLRGN_MUD_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDLRHD_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDLRRH", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDLRRT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSDSILG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSENG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSHSTCF", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSHSTFB", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSHSTFJ", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSMKPLT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSMKR00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSMKRLT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSMODEL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSMRKT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPAM00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPAM00_HIST", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPAM10", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPAMLT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPDT00_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPHD00_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPLI00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPMPLT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPSCSP", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSPSCSP_SYNC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSREBLT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSRETAU_HIST00", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSSECST", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSSERIE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTSTRM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTS_MAG_HISRETAIL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTS_MAG_RETAIL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTS_PDT_CANCEL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.BTS_PRDC_MOQ", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.PA_CAMPG_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.PA_RTLDV_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PA.RB_ORDER_AUDIT", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_PIM.XXRECON_REP_ITEM_CLASSIF_TL_V",
        "pii": False,
    },
    {
        "catalog": "impetus_src",
        "name": "LZ_PIM.XXRECON_REP_ITEM_LANG_TRANS_V",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_PROTIME.ABSENCES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PROTIME.ANOMALIES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PROTIME.ANOMALY_TYPES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PROTIME.CLOCKINGS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PROTIME.EMPLOYEES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_PROTIME.SHIFTS", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_SIEBEL_OLTP.CX_CON_SEGMENTS_CURR",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.CX_MERGE_RECORD", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_SIEBEL_OLTP.CX_MERGE_RECORD_CURR",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.CX_TERM_COND", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.CX_TERM_COND_BU", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.CX_TERM_CONTACT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.CX_TERM_LOV", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ADDR_PER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ADDR_PER_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET_ACCNT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET_ATX", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET_BU", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET_CON", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET_CON_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ASSET_X", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_SIEBEL_OLTP.S_CAMP_CALL_LST_CURR",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CAMP_CON", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CAMP_CON_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_COMMUNICATION", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_SIEBEL_OLTP.S_COMMUNICATION_CURR",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_COMM_DTL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_COMM_SVY_LOG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_ATX_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_BU", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_FNX", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_FNX_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_T_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_X", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CONTACT_X_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CON_ADDR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CON_ADDR_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CON_SM_ATTR_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CON_SM_PROF", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_CON_SM_PROF_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_DOC_AGREE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_EVT_ACT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_EVT_ACT_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_LANG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_MKTSVY_QUES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_NOTE_SR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_NOTE_SR_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_ASSET", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_ASSET_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_CON", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_ORG", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_ORG_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_SRC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_SRC_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_UTX", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_UTX_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_X", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OPTY_X_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORDER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORDER_BU", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORDER_ITEM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_BU", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_BU_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_EXT", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_SIEBEL_OLTP.S_ORG_EXT2_FNX_CURR",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_EXT_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_EXT_FNX", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_EXT_FNX_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_EXT_LSX", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_EXT_X", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_EXT_X_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_MAKE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_PRTNR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_ORG_PRTNR_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_OU_PRTNR_TIER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_PARTY", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_PARTY_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_PARTY_PER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_PARTY_PER_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_POSTN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_PROD_INT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_PROD_INT_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_REVN", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_REVN_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SALES_METHOD", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SRV_REQ", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SRV_REQ_BU", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SRV_REQ_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SRV_REQ_X", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SRV_REQ_XM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SRV_REQ_XM_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SRV_REQ_X_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_SR_ATT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_USER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_USER_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_VHCL_FIN_DTL", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_VHCL_SALES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_VHCL_SALES_BU", "pii": False},
    {
        "catalog": "impetus_src",
        "name": "LZ_SIEBEL_OLTP.S_VHCL_SALES_BU_CURR",
        "pii": False,
    },
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_VHCL_SALES_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_VHCL_SRV", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_VHCL_SRV_CURR", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SIEBEL_OLTP.S_VHCL_SRV_XM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SUNBIL.TBSDB_DICT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SUNBIL.TBSDB_SUNBILHEA", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_SUNBIL.TBSDB_SUNBILPOS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_TSC.TSC_ASSIGNMENT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_TSC.TSC_ASSIGNMENT_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_TSC.TSC_DESC", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_TSC.TSC_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_TSC.TSC_VARIANT_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_TSC.TSC_VARIANT_NSC_SETTING_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMACTIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMCARSMAPPED", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMDATES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMFUELTYPES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMIMAGES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMINTERFACES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMMAPPING", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMMAPPINGTYPES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMPRICES", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UCM.UCMPROVIDERS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UD.ORGANISATIONS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_UD.USERS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VEH_MASTER.BTV14010", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_CHG_ORDER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_ORDADDINF", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_ORDER", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_ORDERROLE", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_ORDER_TYP", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_PROD_STAT", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_STAGE_VPOSTATUS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_VPO.TBVPO_STATUS", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.BOOKING", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.CLAIM", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.OPERATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.PART", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.PROCSTEP", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.SUBLET", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.SUBLOCATION", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.WML_CLAIM_ADJUSTMENT_ID", "pii": False},
    {"catalog": "impetus_src", "name": "LZ_WML.WML_CLAIM_ID", "pii": False},
]

# COMMAND ----------

# work_jsons.extend(work_jsons_source)

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
        "children": wi.get("children", []),
        "run_ts": run_ts,
        "run_name": run_name,
        "partition_count": wi.get("partition_count", cpu_count),
    }
    wi["fqn"] = f"{wi['catalog_name']}.{wi['schema_name']}.{wi['table_name']}"
    return wi


work_items = [create_work_item(wi) for wi in work_jsons]

print(len(work_items))

# COMMAND ----------

# def is_unfinished_task(wi):
#     if wi["action"] == "create" and wi["mode"] == "overwrite" and wi["query_type"] == "dbtable":
#         return not table_exists(wi["catalog_name"], wi["schema_name"], wi["table_name"])
#     else:
#         return True

# work_items = [
#     wi for wi in work_items if is_unfinished_task(wi)
# ]
# print(len(work_items))

# COMMAND ----------

work_items

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
                f"completed {result_dict.get('job_id', 0)}: {work_item.get('fqn', '')}, status_code: {result_dict.get('status_code', -1)}, time_duration: {result_dict.get('time_duration', -1)} sec ({result_dict.get('time_duration', -1)//60} min)."
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


for work_item in work_items:
    q.put(work_item)

create_threads(worker_count)

q.join()

# COMMAND ----------

end_time = time.time()
time_duration = int(end_time - start_time)
print(f"duration notebook seconds: {time_duration}")

# COMMAND ----------

error_results = []
for entry in results:
    try:
        entry = json.loads(entry)
        if entry.get("status_code", -1) >= 300:
            error_results.append(entry)
        # if entry:
        #     pp.pprint(entry)
    except:
        print(entry)

# COMMAND ----------

if error_results:
    dbutils.notebook.exit(json.dumps(error_results))

# COMMAND ----------

dbutils.notebook.exit("{}")
