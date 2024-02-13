# Databricks notebook source
# dbutils.widgets.removeAll()
dbutils.widgets.text("catalog", "DWH_BI1")
dbutils.widgets.text("schema_filter", "like 'LZ_%'")
dbutils.widgets.dropdown("scope", "ACC", ["ACC", "PRD", "DEV"])

# Examples:
# owner LIKE 'LZ_%'
# ( owner LIKE 'LZ_%' OR owner = 'STG'  or
# owner = 'DWH'

# COMMAND ----------

schema_filter = dbutils.widgets.get("schema_filter")
scope = dbutils.widgets.get("scope")
catalog = dbutils.widgets.get("catalog")

hostName = "accdw-scan.mle.mazdaeur.com"
# hostName="10.230.2.32"
port = "1521"
databaseName = f"{scope}_DWH"

jdbcUrl = f"jdbc:oracle:thin:@//{hostName}:{port}/{databaseName}"
print(jdbcUrl)

catalog_name = f"{scope}__{catalog}"
print(catalog_name)

# COMMAND ----------

# TODO: following statement is not accepted by spark SQL, therefore move it to an INIT SQL notebook
sql_catalog_create = f"CREATE CATALOG IF NOT EXISTS {scope}__{catalog} COMMENT 'scope: {catalog}'"
# uses widget values !!!


# COMMAND ----------

def run_sql_cmd(sql:str):
    """The `sql` API only supports statements with no side effects. Supported statements: `SELECT`, `DESCRIBE`, `SHOW TABLES`, `SHOW TBLPROPERTIES`, `SHOW NAMESPACES`, `SHOW COLUMNS IN`, `SHOW FUNCTIONS`, `SHOW VIEWS`, `SHOW CATALOGS`, `SHOW CREATE TABLE`.,None,Map(),Map(),List(),List(),Map())
    """
    print(sql)
    df_cmd = spark.sql(sql)


# COMMAND ----------

# TODO: following statement is not accepted by spark SQL, therefore move it to an INIT SQL notebook
# run_sql_cmd(sql_catalog_create)

# COMMAND ----------

# testing environment and available login credentials
username = dbutils.secrets.get(scope="ACC", key="DWH_BI1__JDBC_USERNAME")
password = dbutils.secrets.get(scope="ACC", key="DWH_BI1__JDBC_PASSWORD")
assert dbutils.secrets.get(
    scope="ACC", key="DWH_BI1__JDBC_USERNAME"
), "secret username not retrieved"
assert dbutils.secrets.get(
    scope="ACC", key="DWH_BI1__JDBC_PASSWORD"
), "secret password not retrieved"

# COMMAND ----------

def get_df_sql(sql):
    df_sql = (
        spark.read.format("jdbc")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("url", jdbcUrl)
        .option("query", sql)
        .option("user", username)
        .option("password", password)
        .option("numPartitions",5) 
        .load()
    )

    # The following options configure parallelism for the query. This is required to get better performance, otherwise only a single thread will read all the data
    # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
    # .option("partitionColumn", "partition_key")
    # lowest value to pull data for with the partitionColumn
    # .option("lowerBound", "minValue")
    # max value to pull data for with the partitionColumn
    # .option("upperBound", "maxValue")
    # number of partitions to distribute the data into. Do not set this very large (~hundreds) to not overwhelm your database
    # .option("numPartitions", <cluster_cores>)

    return df_sql

# COMMAND ----------

def get_df_table(table_name):
    df_sql = (
        spark.read.format("jdbc")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("url", jdbcUrl)
        .option("dbtable", table_name)
        .option("user", username)
        .option("password", password)
        .option("numPartitions",5) 
        .load()
    )

    return df_sql

# COMMAND ----------

sql = f"""SELECT 
OWNER SCHEMA_NAME, TABLE_NAME, cast(NUM_ROWS as INT) NUM_ROWS, cast(AVG_ROW_LEN as INT) AVG_ROW_LEN, (NUM_ROWS * AVG_ROW_LEN)/1024/1024/1024 EST_SIZE_IN_GB
FROM all_tables 
WHERE owner {schema_filter}
AND TABLE_NAME not like 'SNAP_%' and  TABLE_NAME not like '%$%' 
AND NUM_ROWS > 0
ORDER BY NUM_ROWS ASC"""

# COMMAND ----------

df_list = get_df_sql(sql)
display(df_list)

# COMMAND ----------

# not supported by spark sql command, but we don't need it since we use spark with full schema naming
# # sql_catalog_use = f"USE CATALOG {scope}__{catalog}"
# run_sql_cmd(sql_catalog_use)

# COMMAND ----------

# code to get primary key of table if it exists

sql_pk = """SELECT 
  tc.owner, tc.TABLE_NAME, tc.COLUMN_NAME, tc.DATA_TYPE, tc.NULLABLE, tc.NUM_NULLS, tc.NUM_DISTINCT, tc.DATA_DEFAULT, tc.AVG_COL_LEN, tc.CHAR_LENGTH,
  con.cons, ac.CONSTRAINT_NAME, ac.CONSTRAINT_TYPE, ac.STATUS, ac.INDEX_NAME
FROM DBA_TAB_COLUMNS tc
left join
  ( select  listagg( cc.constraint_name, ',') within group (order by cc.constraint_name)  cons, 
         table_name, owner , column_name 
         from  DBA_CONS_COLUMNS cc 
          group by  table_name, owner , column_name ) con
  on con.table_name = tc.table_name and 
     con.owner = tc.owner and
     con.column_name = tc.column_name
left join all_constraints ac
ON tc.owner=ac.owner and tc.TABLE_NAME=ac.TABLE_NAME AND ac.CONSTRAINT_TYPE = 'P' AND con.cons=ac.CONSTRAINT_NAME
where  tc.owner = '{schema}' and  tc.TABLE_NAME = '{table_name}' AND ac.CONSTRAINT_TYPE = 'P'
order by 1 ,2, 3
"""
display(get_df_sql(sql_pk.format(**{"schema": "LZ_MUM", "table_name": "TUSER"})))

# COMMAND ----------

def schema_exists(catalog:str, schema_name:str):
    query = spark.sql(f"""
            SELECT 1 
            FROM {catalog}.information_schema.schemata 
            WHERE schema_name = '{schema_name}' 
            LIMIT 1""")
    
    return query.count() > 0

def table_exists(catalog:str, schema:str, table_name:str):
    query = spark.sql(f"""
            SELECT 1 
            FROM {catalog}.information_schema.tables 
            WHERE table_name = '{table_name}' 
            AND table_schema='{schema}' LIMIT 1""",
        )
    return query.count() > 0

# COMMAND ----------

table_exists( 'ACC__DWH_BI1', 'LZ_LEM', 'DDN_VEHICLE_DISTRIBUTORS')

# COMMAND ----------

# SuperFastPython.com
# example of using starmap() with the thread pool
from random import random
from time import sleep
from multiprocessing.pool import ThreadPool
# import multiprocessing as mp

idx = 0
count = df_list.count()

# task executed in a worker thread
def task(identifier, catalog_name, schema, table_name, scope):
    global idx
    idx += 1
    # catalog_name, schema, table_name, scope = value
    # report a message
    print(f'Task {idx}/{count} {identifier} executing')
    # block for a moment
    result = dbutils.notebook.run("load_table_2", 120, {"catalog_name": catalog_name, "schema": schema, "table_name": table_name, "scope": scope})
    # return the resy
    return (identifier, result)
 

# # create and configure the thread pool
# with ThreadPool() as pool:
#     # prepare arguments
#     items = [(i, random()) for i in range(10)]
#     # execute tasks and thread results in order
#     for result in pool.starmap(task, items):
#         print(f'Got result: {result}')
# # thread pool is closed automatically

# COMMAND ----------

import pprint as pp

task_params = [ (f'{catalog_name}__{row["SCHEMA_NAME"]}__{row["TABLE_NAME"]}', catalog_name, row["SCHEMA_NAME"],  row["TABLE_NAME"] , scope) for row in df_list.collect() ]  

pp.pprint(task_params[0:3])


# COMMAND ----------

# BAD EXAMPLE: parralellized but not distributed. Work is only executed on worker ! 

# callback function
def custom_callback(result_iterable):
	# iterate results
	for result in result_iterable:
		print(f'Got result: {result}')

cpu_count = 16 #   mp.cpu_count()

with ThreadPool(cpu_count) as pool:
    results = pool.starmap_async(task, task_params)
    # iterate results
    for result in results.get():
        print(f'Got result: {result}')
    # # execute tasks and thread results in order
    # for result in pool.starmap(task, task_params):
    #     print(f'Got result: {result}')
# thread pool is closed automatically

# COMMAND ----------

# %sql
# CREATE TABLE acc__dwh_bi1.LZ_MUM.TUSER USING JDBC OPTIONS (
#   url "jdbc:oracle:thin:@//10.230.2.32:1521/ACC_DWH",
#   dbtable "LZ_MUM.TUSER",
#   user secret('ACC', 'DWH_BI1__JDBC_USERNAME'),
#   password secret('ACC', 'DWH_BI1__JDBC_PASSWORD'),
#   driver 'oracle.jdbc.driver.OracleDriver'
# ) AS
# SELECT
#   *
# FROM
#   LZ_MUM.TUSER
