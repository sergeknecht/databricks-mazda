import pytest
from databricks.connect import DatabricksSession

@pytest.fixture(scope="module")
def dbutils():
    from pyspark.dbutils import DBUtils

    spark = DatabricksSession.builder.getOrCreate()
    dbutils= DBUtils(spark)
    yield dbutils
    dbutils = None
    # spark.stop()

# Define a fixture to create a SparkSession on the cluster in the remote
@pytest.fixture(scope="module")
def spark() -> DatabricksSession:
  # Create a SparkSession (the entry point to Spark functionality) on
  # the cluster in the remote Databricks workspace. Unit tests do not
  # have access to this SparkSession by default.
  return DatabricksSession.builder.getOrCreate()


def test_dbutils(dbutils):
    assert dbutils


def test_get_connection_properties__by_key(dbutils, scope: str = "ACC", db_key: str = "DWH_BI1"):
    username = dbutils.secrets.get(
        scope=scope.upper(), key=f"{db_key}__JDBC_USERNAME"
    )
    assert username, "secret username not retrieved"