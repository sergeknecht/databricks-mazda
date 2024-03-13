import pprint as pp

from databricks.sdk.runtime import *

# import the required libraries when running from vcode
try:
    print(type(spark))
except Exception as e:
    print(e)
    from databricks.connect import DatabricksSession
    from databricks.sdk.runtime import *

    spark = DatabricksSession.builder.getOrCreate()


# from pyspark.sql import SparkSession
# from pyspark.sql.types import *


# spark = SparkSession.builder.getOrCreate()

CATALOG = "mle_bi_app_data"
SCHEMA = "log"
TABLE_APPLICATION = "{scope}__application"
FQN = "mle_bi_app_data.log.{scope}__application"


# method that accepts a dic and writes it to a databricks delta table
def log_to_delta(log_dict: dict, catalog: str = CATALOG, schema: str = SCHEMA, table: str = TABLE_APPLICATION):

    if type(log_dict) is not dict:
        raise TypeError("log_dict must be a dictionary")

    if not log_dict:
        return

    # create pandas dataframe from dictionary
    assert "scope" in log_dict, "scope not found in log_dict: " + pp.pformat(log_dict)

    scope = log_dict["scope"].lower()
    table = table.format(scope=scope)
    fqn = "{catalog}.{schema}.{table}".format(scope=scope, catalog=catalog, schema=schema, table=table)

    df = spark.createDataFrame(data=[log_dict])
    # write to delta table
    df.write.format("delta").partitionBy("log_dt").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(fqn)


if __name__ == "__main__":
    scope = "dev"

    log = {"scope": "dev"}
