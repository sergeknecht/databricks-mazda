import threading
import time
from multiprocessing.pool import ThreadPool

import oracledb


def create_db_pool(db_dict: dict[str, str]) -> oracledb.ConnectionPool:
    # create pool using the connection parameters obtained from Databricks secrets or .env file
    return oracledb.create_pool(
        user=db_dict.get("user", None) or db_dict["DB_USERNAME"],
        password=db_dict.get("password", None) or db_dict["DB_PASSWORD"],
        host=db_dict.get("hostName", None) or db_dict["DB_HOST"],
        port=db_dict.get("port", None) or db_dict["DB_PORT"],
        service_name=db_dict.get("databaseName", None) or db_dict["DB_SERVICE_NAME"],
        min=int(db_dict.get("pool_size_min", 2)), # in case value is string
        max=int(db_dict.get("pool_size_max", 8)), # in case value is string
        increment=1,
        getmode=oracledb.POOL_GETMODE_WAIT,
    )


def do_query(
    db_dict: dict[str, str], pool: oracledb.ConnectionPool, __query_id__: int, sql: str
):
    con = pool.acquire()
    assert (
        con.is_healthy()
    ), "Unusable connection. Please check the database and network settings."

    cursor = con.cursor()
    cursor.prefetchrows = int(db_dict.get("fetchSize", 100))  # in case value is string    db_dict.get("fetchSize", 100)
    cursor.arraysize = int(db_dict.get("fetchSize", 100))  # in case value is string    db_dict.get("fetchSize", 100)

    if db_dict.get("SIMULATE_LONG_RUNNING_QUERY", False):
        time.sleep(4)  # sleep seconds to simulate a long running query

    cursor.execute(sql)
    columns = [col.name for col in cursor.description]
    cursor.rowfactory = lambda *args: dict(zip(columns, args))
    results = [row | {"__query_id__": __query_id__} for row in cursor]

    if not results:
        results = [{"__query_id__": __query_id__, "__query_error__": "No results"}]

    if db_dict.get("VERBOSE", False):
        print(
            "__query_id__", __query_id__, threading.current_thread().name, "fetched:", results
        )

    pool.release(con)
    return results


def create_threadpool(db_dict: dict[str, str]):
    print("Creating thread pool with max size:", int(db_dict.get("pool_size_max", 1)))
    t_pool = ThreadPool(processes=int(db_dict.get("pool_size_max", 1)))
    return t_pool


def do_parallel_query(db_dict: dict[str, str], query_task_dict: dict[int, str]):
    db_pool = create_db_pool(db_dict)
    t_pool = create_threadpool(db_dict)
    items = [
        (db_dict, db_pool, __query_id__, sql)
        for __query_id__, sql in query_task_dict.items()
    ]

    results_by_query_id = {}

    # call a function on each item in a list and handle results
    for result in t_pool.starmap(do_query, items):
        # handle the result...
        # print("Result:", result)
        if result:
            # error can be detected by checking if "__query_error__" key exists
            has_error = "__query_error__" in result[0]
            if has_error:
                if db_dict.get("VERBOSE", False):
                    print("Error:", result[0]["__query_error__"])
                results_by_query_id[result[0]["__query_id__"]] = None
            else:
                results_by_query_id[result[0]["__query_id__"]] = result
        else:
            raise Exception("No result returned. There should ALWAYS be a result!")

    return results_by_query_id
