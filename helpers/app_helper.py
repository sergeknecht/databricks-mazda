from helpers.db_helper_delta import create_table, get_or_create_schema
from helpers.dbx_init import spark
from helpers.logger_helper import CATALOG, SCHEMA, TABLE_APPLICATION

def init(scope):

    get_or_create_schema(CATALOG, SCHEMA)
    create_table(CATALOG, SCHEMA, TABLE_APPLICATION.format(scope=scope), spark.createDataFrame([]), overwrite=False)

    # initialize a dictionary
    return {
        "scope": scope
    }

if __name__ == "__main__":
    scope = "dev"
    log = init(scope)
    print(log)