import logging
import json
import pprint as pp

from helpers.db_helper_jdbc_oracle import get_jdbc_url
from helpers.dbx_init import dbutils, spark

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

JDBC_KEYS_ALLOWED = [
    'url',
    'dbtable',
    'user',
    'password',
    'query',
    'driver',
    'partitionColumn',
    'lowerBound',
    'upperBound',
    'numPartitions',
    'fetchSize',
    'pooling',
    'port',
]


def get_db_dict(db_scope: str, db_key: str = 'DWH_BI1', default='DEFAULT'):
    # load file db_configs.json as a dictionary
    with open('./config/db_configs.json') as f:
        db_configs = json.load(f)

    return {**db_configs[db_scope][default], **db_configs[db_scope][db_key]}


def get_connection_properties__by_key(db_scope: str = "ACC", db_key: str = 'DWH_BI1'):

    db_key_base = db_key.split('__')[0]

    username = dbutils.secrets.get(
        scope=db_scope.upper(), key=f'{db_key_base}__JDBC_USERNAME'
    )
    password = dbutils.secrets.get(
        scope=db_scope.upper(), key=f'{db_key_base}__JDBC_PASSWORD'
    )

    assert username, 'secret username not retrieved'
    assert password, 'secret password not retrieved'

    db_dict = get_db_dict(db_scope, db_key)

    url = get_jdbc_url(db_dict)

    # return conn_dict merged with extra values
    return {
        **db_dict,
        'user': username,
        'password': password,
        'url': url,
    }

# DO NOT USE THIS FUNCTION
def get_data(
    db_dict: dict, table_name: str, query_type='dbtable'  # SQL SELECT STATEMENT
):
    # TODO: get attribute name from primary key and use it as the partitionColumn or order by

    df = (
        spark.read.format('jdbc').option(query_type, table_name).options(**db_dict).load()
    )

    return df


def get_jdbc_data_by_dict(
    db_conn_props: dict,
    work_item: dict,
):
    query_type = work_item['query_type']
    query_sql = (
        work_item['query_sql'] if query_type == 'query' else work_item['table_sql']
    )

    # # with partitioning query is not allowed
    # if 'partitionColumn' in db_conn_props:
    #     query_type = 'dbtable'



    db_conn_props = {k: v for k, v in db_conn_props.items() if k in JDBC_KEYS_ALLOWED}
    db_conn_props = {k: v for k, v in db_conn_props.items() if v}

    logger.info(query_sql)
    logger.info(pp.pformat(db_conn_props))

    df = (
        spark.read.format('jdbc')
        .option(query_type, query_sql)
        .options(**db_conn_props)
        # .load()
    )

    return df

def get_jdbc_data_by_dict__by_partition_key(
    db_conn_props: dict,
    work_item: dict,
    bounds,
    column_name_partition:str,
    partition_count: int = 4,
):
    logger.info(f"{work_item['table_sql']}: partitionColumn: {column_name_partition}, numPartitions: {partition_count}, bounds: {bounds.MIN_ID:.0f} - {bounds.MAX_ID+1:.0f}")
    # lowest value to pull data for with the partitionColumn
    db_conn_props['lowerBound'] = f'{bounds.MIN_ID:.0f}'
    # max value to pull data for with the partitionColumn
    db_conn_props['upperBound'] = f'{bounds.MAX_ID+1:.0f}'
    # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
    db_conn_props["partitionColumn"] = column_name_partition
    # number of partitions to distribute the data into. Do not set this very large (~hundreds)
    db_conn_props["numPartitions"] = partition_count

    return get_jdbc_data_by_dict(db_conn_props, work_item)

def get_jdbc_bounds__by_partition_key(
    db_conn_props: dict,
    table_name: str,  # example: "LZ_MUM.TUSER"
    column_name_partition: str,  # example: "USER_ID"
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
    SELECT MIN({column_name_partition}) as MIN_ID, MAX({column_name_partition}) as MAX_ID
    FROM {table_name}
    ) dataset
    """

    logger.info(pp.pformat(db_conn_props))

    bounds = spark.read.jdbc(
        url=db_conn_props['url'],
        table=pushdown_query,
        properties=db_conn_props,
    ).collect()[0]

    logger.info(pp.pformat(bounds))

    return bounds

def get_jdbc_bounds__by_rownum(
    db_conn_props: dict,
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
        url=db_conn_props['url'],
        table=pushdown_query,
        properties=db_conn_props,
    ).collect()[0]

    return bounds
