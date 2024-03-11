from databricks.sdk.runtime import *

from helpers.db_helper_delta import create_or_append_table, get_or_create_schema
from helpers.dbx_init import spark
from helpers.logger_helper import CATALOG, SCHEMA, TABLE_APPLICATION
from helpers.status_helper import create_status


def init(scope, catalog=CATALOG, schema=SCHEMA, table=TABLE_APPLICATION):
    scope = scope.lower()
    result = create_status(scope=scope, status_message= "START_APP", status_code= 200)
    fqn = f"{catalog}.{schema}.{scope}__application"
    result["fqn"] = fqn.format(scope=scope)

    get_or_create_schema(catalog, schema)

    create_or_append_table(catalog, schema, table.format(scope=scope), df=spark.createDataFrame([result]), partition_cols=["log_dt"], overwrite=False)

    # initialize a dictionary
    return result

if __name__ == "__main__":
    scope = "dev"
    result = init(scope)
    print(result)
