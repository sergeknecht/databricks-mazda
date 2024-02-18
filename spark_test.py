import pytest
from pyspark.sql import SparkSession


# Define a fixture to create a SparkSession on the cluster in the remote
@pytest.fixture(scope='module')
def spark() -> SparkSession:
    # Create a SparkSession (the entry point to Spark functionality) on
    # the cluster in the remote Databricks workspace. Unit tests do not
    # have access to this SparkSession by default.
    return SparkSession.builder.getOrCreate()


# Now add your unit tests.

# For example, here is a unit test that must be run on the
# cluster in the remote Databricks workspace.
# This example determines whether the specified cell in the
# specified table contains the specified value. For example,
# the third column in the first row should contain the word "Ideal":
#
# +----+-------+-------+-------+---------+-------+-------+-------+------+-------+------+
# |_c0 | carat | cut   | color | clarity | depth | table | price | x    | y     | z    |
# +----+-------+-------+-------+---------+-------+-------+-------+------+-------+------+
# | 1  | 0.23  | Ideal | E     | SI2     | 61.5  | 55    | 326   | 3.95 | 3. 98 | 2.43 |
# +----+-------+-------+-------+---------+-------+-------+-------+------+-------+------+
# ...
#
def test_spark(spark):
    spark.sql('USE catalog samples')
    spark.sql('USE tpch')
    data = spark.sql(
        'SELECT c_custkey, c_name FROM customer WHERE c_custkey = 412452 LIMIT 1'
    )
    assert data.collect()[0][1] == 'Customer#000412452'
