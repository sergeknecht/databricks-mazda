import pytest 

from helpers.db_helper import get_connection_properties__by_key
from databricks.connect import DatabricksSession
from pyspark.dbutils import DBUtils

spark = DatabricksSession.builder.getOrCreate()
dbutils= DBUtils(spark)


# @pytest.fixture(scope="session")
# def dbutils():

#     dbutils = None

#     if spark.conf.get("spark.databricks.service.client.enabled") == "true":

#         from pyspark.dbutils import DBUtils

#         dbutils = DBUtils(spark)

#     else:

#         import IPython

#         dbutils = IPython.get_ipython().user_ns["dbutils"]

#     return dbutils

def test_dbutils(dbutils):
    assert dbutils

def test_connection_properties__by_key(
    scope: str = "ACC", db_key: str = "DWH_BI1"
):
    conn_props = get_connection_properties__by_key(scope, db_key)
    assert conn_props["user"]
    assert conn_props["password"]
    assert conn_props["url"]
    assert conn_props["driver"]
    assert conn_props["fetchSize"]
    assert conn_props["scope"]
    assert conn_props["db_key"]
    assert conn_props["hostName"]
    assert conn_props["port"]
    assert conn_props["databaseName"]
    assert conn_props["db_type"]


if __name__ == "__main__":
    test_connection_properties__by_key()
    print("helpers/db_helper.py is OK")
