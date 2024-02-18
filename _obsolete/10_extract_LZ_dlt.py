# Databricks notebook source
from pyspark.sql import SparkSession


def get_parameter_or_return_default(
    parameter_name: str = 'pipeline.parameter_name',
    default_value: str = 'default_value',
) -> str:
    try:
        spark = SparkSession.getActiveSession()
        parameter = spark.conf.get(parameter_name)
    except Exception:
        print('Caught NoSuchElementException: Property {parameter_name} does not exist.')
        parameter = default_value
    return parameter


scope = get_parameter_or_return_default('pipeline.scope', None)
assert scope, f'scope not set: {scope}'

# COMMAND ----------

dbutils.widgets.removeAll()


# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


def append_ingestion_columns(_df: DataFrame):
    return _df.withColumn('ingestion_timestamp', current_timestamp()).withColumn(
        'ingestion_date', current_date()
    )


# COMMAND ----------

hostName = 'accdw-scan.mle.mazdaeur.com'
# hostName="10.230.2.32"
port = '1521'
databaseName = f'{scope}_DWH'

jdbcUrl = f'jdbc:oracle:thin:@//{hostName}:{port}/{databaseName}'
print(jdbcUrl)

# COMMAND ----------

# testing environment and available login credentials
username = dbutils.secrets.get(scope='ACC', key='DWH_BI1__JDBC_USERNAME')
password = dbutils.secrets.get(scope='ACC', key='DWH_BI1__JDBC_PASSWORD')
assert dbutils.secrets.get(
    scope='ACC', key='DWH_BI1__JDBC_USERNAME'
), 'secret username not retrieved'
assert dbutils.secrets.get(
    scope='ACC', key='DWH_BI1__JDBC_PASSWORD'
), 'secret password not retrieved'

# COMMAND ----------


def get_df_table(table_name):
    df_sql = (
        spark.read.format('jdbc')
        .option('driver', 'oracle.jdbc.driver.OracleDriver')
        .option('url', jdbcUrl)
        .option('dbtable', table_name)
        .option('user', username)
        .option('password', password)
        .load()
    )

    return df_sql


# COMMAND ----------


def extract_source(source_name, target_name=None):
    if target_name is None:
        target_name = source_name.lower()

    @dlt.table(
        name=target_name,
        # comment="<comment>",
        # spark_conf={"<key>" : "<value", "<key" : "<value>"},
        # table_properties={"<key>" : "<value>", "<key>" : "<value>"},
        # path="<storage-location-path>",
        # partition_cols=["<partition-column>", "<partition-column>"],
        # schema="schema-definition",
        temporary=False,
    )
    # @dlt.expect
    # @dlt.expect_or_fail
    # @dlt.expect_or_drop
    # @dlt.expect_all
    # @dlt.expect_all_or_drop
    # @dlt.expect_all_or_fail
    def loard_src_into_raw():
        return get_df_table(source_name)


# COMMAND ----------

extract_source('LZ_OSB.EVENTS')
