import pprint as pp

from helpers.local.environ_helper import EnvironHelper
from helpers.oracledb_helper import do_parallel_query

ENV_VARS = ["DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]
env_helper = EnvironHelper(filename=".env")
db_dict = env_helper.get_env_vars(ENV_VARS)
db_dict["pool_size_max"] = 3
db_dict["SIMULATE_LONG_RUNNING_QUERY"] = True
db_dict["VERBOSE"] = True


task_count = 5
sql = "select sysdate from dual"
query_tasks = {i: sql for i in range(task_count)}
results_by_query_id = do_parallel_query(db_dict, query_tasks)
pp.pprint(results_by_query_id)
print("All done!")
