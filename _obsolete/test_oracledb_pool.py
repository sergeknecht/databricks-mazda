import threading
import time

import oracledb

from helpers.local.environ_helper import EnvironHelper

ENV_VARS = ["DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]
env_helper = EnvironHelper(filename=".env")
env_vars = env_helper.get_env_vars(ENV_VARS)


def create_pool(env_vars: dict[str, str], max:int) -> oracledb.ConnectionPool:
    return oracledb.create_pool(
        user=env_vars["DB_USERNAME"],
        password=env_vars["DB_PASSWORD"],
        host=env_vars["DB_HOST"],
        port=env_vars["DB_PORT"],
        service_name=env_vars["DB_SERVICE_NAME"],
        min=2,
        max=max,
        increment=1,
        getmode=oracledb.POOL_GETMODE_WAIT,
    )


def Query(pool: oracledb.ConnectionPool, idx:int, sql: str = "select sysdate from dual"):
    con = pool.acquire()
    assert con.is_healthy(), "Unusable connection. Please check the database and network settings."

    cursor = con.cursor()
    cursor.prefetchrows = 100
    cursor.arraysize = 100

    time.sleep(4) # sleep seconds to simulate a long running query

    cursor.execute(sql)
    columns = [col.name for col in cursor.description]
    cursor.rowfactory = lambda *args: dict(zip(columns, args))
    results = [row for row in cursor]
    # seqval, = cur.fetchone()
    print("Thread", idx, threading.current_thread().name, "fetched:", results)

    pool.release(con)
    return results

task_count = 8
pool_size_max = 2
threadArray = []

pool = create_pool(env_vars, max=pool_size_max)

for i in range(task_count):
    thread = threading.Thread(name='#' + str(i), target=Query, args=(pool, i))
    threadArray.append(thread)
    thread.start()

for t in threadArray:
    t.join()

print("All done!")
