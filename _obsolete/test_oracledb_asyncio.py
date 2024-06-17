import asyncio
import pprint as pp

import oracledb

from helpers.asyncio_helper import run_parallel, run_sequence
from helpers.local.environ_helper import EnvironHelper

ENV_VARS = ["DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]
env_helper = EnvironHelper(filename=".env")
env_vars = env_helper.get_env_vars(ENV_VARS)

print("Synchronous connection...")

def connect_synchronous(env_vars: dict[str, str], sql: str = "select sysdate from dual"):
    with oracledb.connect(
        user=env_vars["DB_USERNAME"],
        password=env_vars["DB_PASSWORD"],
        host=env_vars["DB_HOST"],
        port=env_vars["DB_PORT"],
        service_name=env_vars["DB_SERVICE_NAME"],
    ) as con:
        assert con.is_healthy(), "Unusable connection. Please check the database and network settings."

        cursor = con.cursor()
        cursor.prefetchrows = 100
        cursor.arraysize = 100
        # for row in cursor.execute("select sysdate from dual"):
        #     print(row)

        # print("Fetch each row as a Dictionary")
        cursor.execute(sql)
        columns = [col.name for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        results = [row for row in cursor]

        return results


results = connect_synchronous(env_vars)

pp.pprint(results)

print("Synchronous connection with pool...")

def connect_synchronous_pool(env_vars: dict[str, str], sql: str = "select sysdate from dual"):
    with oracledb.connect(
        user=env_vars["DB_USERNAME"],
        password=env_vars["DB_PASSWORD"],
        host=env_vars["DB_HOST"],
        port=env_vars["DB_PORT"],
        service_name=env_vars["DB_SERVICE_NAME"],
    ) as con:
        assert con.is_healthy(), "Unusable connection. Please check the database and network settings."

        cursor = con.cursor()
        cursor.prefetchrows = 100
        cursor.arraysize = 100
        # for row in cursor.execute("select sysdate from dual"):
        #     print(row)

        # print("Fetch each row as a Dictionary")
        cursor.execute(sql)
        columns = [col.name for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        results = [row for row in cursor]

        return results


results = connect_synchronous(env_vars)

pp.pprint(results)

# print("Asynchronous connection...")

async def connect_asynchronous(env_vars: dict[str, str], sql: str = "select sysdate from dual"):
    async with oracledb.connect_async(
        user=env_vars["DB_USERNAME"],
        password=env_vars["DB_PASSWORD"],
        dsn=env_vars["DB_HOST"] + "/" + env_vars["DB_SERVICE_NAME"],
        # host=env_vars["DB_HOST"],
        # port=env_vars["DB_PORT"],
        # service_name=env_vars["DB_SERVICE_NAME"],
        # expire_time=5,
        tcp_connect_timeout=5,
        disable_oob=True,
    ) as con:
        if await con.ping():
            print("Ping successful!")
        else:
            print("Ping failed. Please check the database and network settings.")

        if await con.is_healthy():
            print("Healthy connection!")
        else:
            print("Unusable connection. Please check the database and network settings.")

        # con.call_timeout = 10000 # milliseconds
        # print("Call timeout set at", con.call_timeout, "milliseconds...")

        async with con.cursor() as cursor:
            cursor.prefetchrows = 100
            cursor.arraysize = 100

            await cursor.execute(sql)
            # async for result in cursor:
            #     print(result)
            (today,) = await cursor.fetchone(sql)
            print("Fetch of current date before timeout:", today)

# asyncio.run(connect_asynchronous(env_vars))


exit(0)

# Number of coroutines to run
CONCURRENCY = 5

# Query the unique session identifier/serial number combination of a connection
SQL = """SELECT UNIQUE CURRENT_TIMESTAMP AS CT, sid || '-' || serial# AS SIDSER
         FROM v$session_connect_info
         WHERE sid = SYS_CONTEXT('USERENV', 'SID')"""


# Show the unique session identifier/serial number of each connection that the
# pool opens
async def init_session(connection, requested_tag):
    res = await connection.fetchone(SQL)
    print(res[0].strftime("%H:%M:%S.%f"), "- init_session SID-SERIAL#", res[1])


# The coroutine simply shows the session identifier/serial number of the
# connection returned by the pool.acquire() call
async def query(pool):
    async with pool.acquire() as connection:
        await connection.callproc("dbms_session.sleep", [1])
        res = await connection.fetchone(SQL)
        print(res[0].strftime("%H:%M:%S.%f"), "- query SID-SERIAL#", res[1])


async def main():
    ENV_VARS = ["DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]

    env_helper = EnvironHelper(filename=".env")
    env_vars = env_helper.get_env_vars(ENV_VARS)
    connection_string = get_connect_string(env_vars)

    pool = oracledb.create_pool_async(
        user=env_vars["DB_USERNAME"],
        password=env_vars["DB_PASSWORD"],
        dsn=connection_string,
        min=1,
        max=CONCURRENCY,
        session_callback=init_session,
        timeout=5,
        wait_timeout=5,
    )

    coroutines = [query(pool) for i in range(CONCURRENCY)]

    await asyncio.gather(*coroutines)

    await pool.close()


asyncio.run(main())
