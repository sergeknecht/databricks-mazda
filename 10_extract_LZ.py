# Databricks notebook source
dbutils.widgets.removeAll()
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

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${scope}__${catalog} COMMENT "scope: ${catalog}";
# MAGIC -- uses widget values !!!

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

# MAGIC %sql
# MAGIC USE CATALOG ${scope}__${catalog}

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

def perform_task(idx, idx_max, catalog_name, schema, table_name):
    # Construct the fully-qualified table name
    dbx_qualified_table_name = f"{catalog_name}.{schema}.{table_name}"
    qualified_table_name = f"{schema}.{table_name}"

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema} WITH DBPROPERTIES (Scope='{scope}')"
    )

    if not table_exists(catalog_name, schema, table_name):
      #     spark.sql(f"DROP TABLE {qualified_table_name}")

      # if not spark.catalog.tableExists(qualified_table_name):
      df = get_df_table(qualified_table_name)
      print(f"writing: {idx} of {idx_max} " + dbx_qualified_table_name)
      df.write.format("delta").mode("overwrite").saveAsTable(dbx_qualified_table_name)
      print("\tOK")

      sql_pk_table = sql_pk.format(**{"schema": schema, "table_name": table_name})
      df_pk = get_df_sql(sql_pk_table)
      for row_pk in df_pk.collect():
          column_name = row_pk["COLUMN_NAME"]
          sqls = [
              f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET NOT NULL",
              f"ALTER TABLE {dbx_qualified_table_name} DROP PRIMARY KEY IF EXISTS CASCADE",
              f"ALTER TABLE {dbx_qualified_table_name} ADD CONSTRAINT pk_{table_name}_{column_name} PRIMARY KEY({column_name})",
              f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')"
          ]
          for curr_sql in sqls:
              print("\t", curr_sql)
              spark.sql(curr_sql)


# COMMAND ----------

from pyspark.sql.utils import AnalysisException

count = 0
count_max = 0
for row in df_list.collect():
    count +=1
    count_max = df_list.count()

    schema = row["SCHEMA_NAME"]
    table_name = row["TABLE_NAME"]


    result = dbutils.notebook.run("load_table", 60, {"idx":str(count), "idx_max": str(count_max), "catalog_name": catalog_name, "schema": schema, "table_name": table_name, "scope": scope})
    print(result)

    

    # %run ./load_table $idx=str(count) $idx_max=str(count_max) $catalog_name=catalog_name $schema=schema $table_name=table_name $scope=scope

    # # Construct the fully-qualified table name
    # dbx_qualified_table_name = f"{catalog_name}.{schema}.{table_name}"
    # qualified_table_name = f"{schema}.{table_name}"

    # try:
    #     # Check if table exists in Spark catalog
    #     spark.table(dbx_qualified_table_name)
    # except AnalysisException:
    #     # Handle exception if table does not exist
    #     print(f"Table {dbx_qualified_table_name} does not exist.")
    # else:
    #     # Get schema of table from Spark catalog
    #     df = spark.read.table(dbx_qualified_table_name)
    #     schema = df.schema
    #     print(schema)



# COMMAND ----------

count = 0
count_max = 0
for row in df_list.collect():
    count +=1
    count_max = df_list.count()

    schema = row["SCHEMA_NAME"]
    table_name = row["TABLE_NAME"]
    # perform_task(count, count_max, catalog_name, schema, table_name)

    # Construct the fully-qualified table name
    dbx_qualified_table_name = f"{catalog_name}.{schema}.{table_name}"
    qualified_table_name = f"{schema}.{table_name}"

    spark.sql(
        f"USE CATALOG {catalog_name}"
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema} WITH DBPROPERTIES (Scope='{scope}')"
    )

    if not table_exists(catalog_name, schema, table_name):
        #     spark.sql(f"DROP TABLE {qualified_table_name}")

        # if not spark.catalog.tableExists(qualified_table_name):
        df = get_df_table(qualified_table_name)
        if df.count() > 0:
            print(f"writing: {count} of {count_max} " + dbx_qualified_table_name)
            df.write.format("delta").mode("overwrite").saveAsTable(dbx_qualified_table_name)
            print("\tOK")

            sql_pk_table = sql_pk.format(**{"schema": schema, "table_name": table_name})
            df_pk = get_df_sql(sql_pk_table)

            for row_pk in df_pk.collect():
                column_name = row_pk["COLUMN_NAME"]
                sqls = [
                    f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET NOT NULL",
                    f"ALTER TABLE {dbx_qualified_table_name} DROP PRIMARY KEY IF EXISTS CASCADE",
                    f"ALTER TABLE {dbx_qualified_table_name} ADD CONSTRAINT pk_{table_name}_{column_name} PRIMARY KEY({column_name})",
                    f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')"
                ]
                for curr_sql in sqls:
                    print("\t", curr_sql)
                    spark.sql(curr_sql)

    

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL LZ_MUM.TUSER

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED LZ_MUM.TUSER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE acc__dwh_bi1.LZ_MUM.TUSER USING JDBC OPTIONS (
# MAGIC   url "jdbc:oracle:thin:@//10.230.2.32:1521/ACC_DWH",
# MAGIC   dbtable "LZ_MUM.TUSER",
# MAGIC   user secret('ACC', 'DWH_BI1__JDBC_USERNAME'),
# MAGIC   password secret('ACC', 'DWH_BI1__JDBC_PASSWORD'),
# MAGIC   driver 'oracle.jdbc.driver.OracleDriver'
# MAGIC ) AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   LZ_MUM.TUSER
