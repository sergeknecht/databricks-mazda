import datetime

from databricks.sdk.runtime import *

# import the required libraries when running from vcode
try:
    print(type(spark))
except Exception as e:
    print(e)
    from databricks.connect import DatabricksSession
    from databricks.sdk.runtime import *

    spark = DatabricksSession.builder.getOrCreate()


def create_status(
    scope: str, status_message: str = "", status_code: int = 200, status_ctx: dict = None
) -> dict:
    assert scope, "scope is required"

    status_ts = datetime.datetime.now(tz=datetime.timezone.utc).replace(
        microsecond=0
    )  # .isoformat()
    status_dt = status_ts.date().isoformat()
    status_ts = status_ts.isoformat()
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        result = {
            "status_code": status_code,
            "status_message": status_message,
            "log_ts": status_ts,
            "log_dt": status_dt,
            "time_duration": -1,
            "scope": scope,
            "user_name": ctx.userName().get() if ctx.userName() else None,
            "nb_id": ctx.notebookId().get() if ctx.notebookId() else "-1",
            "nb_path": ctx.notebookPath().get() if ctx.notebookPath() else None,
            "cluster_id": ctx.clusterId().get() if ctx.clusterId() else None,
        }
    except Exception as e:
        print(__name__, e)
        result = {
            "status_code": status_code,
            "status_message": status_message,
            "log_ts": status_ts,
            "log_dt": status_dt,
            "time_duration": -1,
            "scope": scope,
        }

    if status_ctx:
        context_keys = ["mode", "fqn", "db_key", "run_ts", "run_name", "job_id", "partition_count"]
        for ck in context_keys:
            if ck in status_ctx:
                result[ck] = status_ctx[ck]

    return result


if __name__ == "__main__":
    print(create_status())
