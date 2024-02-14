import json

from helpers.dbx_init import (spark, dbutils)

JDBC_KEYS_ALLOWED = ["url", "dbtable", "user", "password", "query", "driver", "partitionColumn", "lowerBound", "upperBound", "numPartitions", "fetchSize", "pooling", "port"]

def get_db_dict(scope: str, db_key: str = "DWH_BI1", default="DEFAULT"):
    # load file db_configs.json as a dictionary
    with open("./config/db_configs.json") as f:
        db_configs = json.load(f)

    return {**db_configs[scope][default], **db_configs[scope][db_key]}


def get_jdbc_url(db_dict: dict):
    if db_dict["db_type"] == "oracle":
        return (
            f"jdbc:oracle:thin:@//{db_dict['hostName']}:"
            f"{db_dict['port']}/{db_dict['databaseName']}"
        )
    else:
        raise ValueError(f"db_type {db_dict['db_type']} not supported")


def get_connection_properties__by_key(scope: str, db_key: str = "DWH_BI1"):

    db_key_base = db_key.split("__")[0]

    username = dbutils.secrets.get(
        scope=scope.upper(), key=f"{db_key_base}__JDBC_USERNAME"
    )
    password = dbutils.secrets.get(
        scope=scope.upper(), key=f"{db_key_base}__JDBC_PASSWORD"
    )

    assert username, "secret username not retrieved"
    assert password, "secret password not retrieved"

    db_dict = get_db_dict(scope, db_key)

    url = get_jdbc_url(db_dict)

    # return conn_dict merged with extra values
    return {
        **db_dict,
        # "scope": scope,
        # "db_key": db_key,
        "user": username,
        "password": password,
        "url": url,
    }


def get_data(
    db_dict: dict,
    table_name: str,  # SQL SELECT STATEMENT
    query_type = "dbtable"
):
    # TODO: get attribute name from primary key and use it as the partitionColumn or order by

    df = (
        spark.read.format("jdbc")
        .option(query_type, table_name)
        .options(**db_dict)
        .load()
    )

    return df

def get_jdbc_data_by_dict(
    db_conn_props: dict,
    work_item: dict,
):
    query_type = work_item["query_type"]
    query_sql = work_item["query_sql"] if query_type == "query" else work_item["table_sql"]
    db_conn_props = {k:v for k,v in db_conn_props.items() if k in JDBC_KEYS_ALLOWED}
    # import pprint as pp
    # pp.pprint(db_conn_props)
    # print(query_type, query_sql)
    df = (  
        spark.read.format("jdbc")
        .option(query_type, query_sql)
        .options(**db_conn_props)
        .load()
    )

    return df

def get_bounds__by_rownum(
    db_dict: dict,
    table_name: str,  # example: "LZ_MUM.TUSER"
):
    """
    Retrieves the minimum and maximum ID values from a specified table.
    Uses the ROWNUM pseudo-column to retrieve the bounds.
    when we have no uniform distributed attribute of type int.

    Args:
        db_dict (dict): Dictionary containing the database connection details.
        table_name (str): schema.table_name to retrieve the bounds from.

    Returns:
        tuple: A tuple containing the minimum and maximum ID values.

    Example:
        bounds = get_bounds__by_rownum(db_dict, "LZ_MUM.TUSER")
        print(bounds)  # Output: (1, 100)
    """
    pushdown_query = f"""(
    select  1 as MIN_ID, count(*) as MAX_ID
    from {table_name}
    ) dataset
    """

    bounds = spark.read.jdbc(
        url=db_dict["url"],
        table=pushdown_query,
        properties=db_dict,
    ).collect()[0]

    return bounds


def get_data_partitioned__by_rownum(
    db_dict: dict,
    table_name: str,  # example: "LZ_MUM.TUSER"
    bounds,
    numPartitions: int = 4,
    order_by_column: str = "",
):
    if order_by_column:
        order_by = f"order by d.{order_by_column}"
    else:
        order_by = ""

    pushdown_query = f"""(
    select ROWNUM, d.*
    from {table_name} d
    {order_by}
    ) dataset
    """

    df = (
        spark.read.format("jdbc")
        # .option("url", jdbcUrl)
        .option("dbtable", pushdown_query)
        # .option("driver", driver)
        # .option("user", username)
        # .option("password", password)
        # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
        .option("partitionColumn", "ROWNUM")
        # lowest value to pull data for with the partitionColumn
        .option("lowerBound", f"{bounds.MIN_ID:.0f}")
        # max value to pull data for with the partitionColumn
        .option("upperBound", f"{bounds.MAX_ID+1:.0f}")
        # number of partitions to distribute the data into. Do not set this very large (~hundreds)
        .option("numPartitions", numPartitions)
        # # Oracle’s default fetchSize is 10
        # .option("fetchSize", "100")
        .options(**db_dict).load()
    )

    return df


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
