import json

def get_db_dict(scope: str, db_key: str = "DWH_BI1", default="DEFAULT"):
    # load file db_configs.json as a dictionary
    with open("db_configs.json") as f:
        db_configs = json.load(f)

    return {**db_configs[default], **db_configs[scope][db_key]}


def get_jdbc_url(db_dict: dict):
    if db_dict["db_type"] == "oracle":
        return (
            f"jdbc:oracle:thin:@//{db_dict['hostName']}:"
            f"{db_dict['port']}/{db_dict['databaseName']}"
        )
    else:
        raise ValueError(f"db_type {db_dict['db_type']} not supported")


def get_connection_properties__by_key(scope: str, db_key: str = "DWH_BI1"):

    username = dbutils.secrets.get(
        scope=scope.upper(), key=f"{db_key}__JDBC_USERNAME"
    )
    password = dbutils.secrets.get(
        scope=scope.upper(), key=f"{db_key}__JDBC_PASSWORD"
    )

    assert username, "secret username not retrieved"
    assert password, "secret password not retrieved"

    db_dict = get_db_dict(scope, db_key)

    url = get_jdbc_url(db_dict)

    # return conn_dict merged with extra values
    return {
        **db_dict,
        "scope": scope,
        "db_key": db_key,
        "user": username,
        "password": password,
        "url": url,
    }


def get_data(
    db_dict: dict,
    table_name: str,  # SQL SELECT STATEMENT
):
    # TODO: get attribute name from primary key and use it as the partitionColumn or order by

    df = (
        spark.read.format("jdbc")
        .option("dbtable", table_name)
        .options(**db_dict)
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
