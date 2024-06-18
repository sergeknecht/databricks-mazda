import pprint as pp

from helpers.local.environ_helper import EnvironHelper
from helpers.oracledb_helper import do_parallel_query

ENV_VARS = ["DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]
env_helper = EnvironHelper(filename=".env")
db_dict = env_helper.get_env_vars(ENV_VARS)
db_dict["pool_size_max"] = 2
db_dict["SIMULATE_LONG_RUNNING_QUERY"] = False
db_dict["VERBOSE"] = True


# Create a dictionary of tasks to be executed in parallel
# key is an identifier (int) for the query
# value is the SQL query to be executed
task_count = 6
sql = "select sysdate from dual"
query_tasks = [{"__query_id__": id, "sql": sql} for id in range(task_count)]

print("Tasks to be executed in parallel:")
pp.pprint(query_tasks)

results_by_query_id = do_parallel_query(db_dict, query_tasks)

print("Results of parallel queries:")
pp.pprint(results_by_query_id)
