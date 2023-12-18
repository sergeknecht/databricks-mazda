# COMMAND ----------


from pyspark.sql.utils import AnalysisException

# COMMAND ----------


def extract_table(catalog_name, schema, table_name, scope):
    hostName = "accdw-scan.mle.mazdaeur.com"
    # hostName="10.230.2.32"
    port = "1521"
    databaseName = f"{scope}_DWH"

    jdbcUrl = f"jdbc:oracle:thin:@//{hostName}:{port}/{databaseName}"
    print(jdbcUrl)

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


def schema_exists(catalog: str, schema_name: str):
    query = spark.sql(
        f"""
            SELECT 1 
            FROM {catalog}.information_schema.schemata 
            WHERE schema_name='{schema_name}' 
            LIMIT 1"""
    )

    return query.count() > 0


def table_exists(catalog: str, schema: str, table_name: str):
    query = spark.sql(
        f"""
            SELECT 1 
            FROM {catalog}.information_schema.tables 
            WHERE table_name=lower('{table_name}') 
            AND table_schema=lower('{schema}') LIMIT 1"""
    )
    return query.count() > 0


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
# display(get_df_sql(sql_pk.format(**{"schema": "LZ_MUM", "table_name": "TUSER"})))

# COMMAND ----------

# check_result = table_exists(catalog_name, schema, table_name)
# if check_result:
#     dbutils.notebook.exit(f"SKIPPED: {dbx_qualified_table_name}")
# else:
#     dbutils.notebook.exit(f"created: {dbx_qualified_table_name}")


# COMMAND ----------


def perform_task(catalog_name, schema, table_name):
    # Construct the fully-qualified table name
    dbx_qualified_table_name = f"{catalog_name}.{schema}.{table_name}"
    qualified_table_name = f"{schema}.{table_name}"

    if not table_exists(catalog_name, schema, table_name):
        #     spark.sql(f"DROP TABLE {qualified_table_name}")
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema} WITH DBPROPERTIES (Scope='{scope}')"
        )

        # if not spark.catalog.tableExists(qualified_table_name):
        df = get_df_table(qualified_table_name)
        print(f"writing: " + dbx_qualified_table_name)
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
                f"ALTER TABLE {dbx_qualified_table_name} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')",
            ]
            for curr_sql in sqls:
                print("\t", curr_sql)
                spark.sql(curr_sql)
        return f"created: {dbx_qualified_table_name}"
    else:
        return f"SKIPPED: {dbx_qualified_table_name}"


# COMMAND ----------


dbx_qualified_table_name = f"{catalog_name}.{schema}.{table_name}"
qualified_table_name = f"{schema}.{table_name}"

try:
    # Check if table exists in Spark catalog
    # spark.table(dbx_qualified_table_name)
    result = perform_task(catalog_name, schema, table_name)
    dbutils.notebook.exit(result)
except AnalysisException as ex:
    # Handle exception if table does not exist
    # print(f"Table {dbx_qualified_table_name} does not exist.")
    dbutils.notebook.exit(ex)
# else:
#     dbutils.notebook.exit(f"Table {dbx_qualified_table_name} EXISTS.")
# # Get schema of table from Spark catalog
# df = spark.read.table(dbx_qualified_table_name)
# schema = df.schema
# print(schema)
