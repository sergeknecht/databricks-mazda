from helpers.db_helper_delta import create_or_append_table, get_or_create_schema
from helpers.dbx_init import spark
from helpers.logger_helper import CATALOG, SCHEMA, TABLE_APPLICATION, FQN
from helpers.status_helper import create_status
from databricks.sdk.runtime import *


def init(scope):
    scope = scope.lower()
    result = create_status(scope=scope, status_message= "START_APP", status_code= 200)
    result["fqn"] = FQN.format(scope=scope)
    # log_to_delta_table(result)

    get_or_create_schema(CATALOG, SCHEMA)
    
    create_or_append_table(CATALOG, SCHEMA, TABLE_APPLICATION.format(scope=scope), df=spark.createDataFrame([result]), partition_cols=["log_dt"], overwrite=False)

    # initialize a dictionary
    return result

if __name__ == "__main__":
    scope = "dev"
    result = init(scope)
    print(result)
