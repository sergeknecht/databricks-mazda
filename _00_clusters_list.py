from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State

w = WorkspaceClient()

for c in w.clusters.list():
    if c.state == State.TERMINATED:
        # print("Deleting cluster: {}".format(c.cluster_name))
        # w.clusters.delete(c.cluster_id)
        continue

    print(c.state, c.state_message)

    print(c.cluster_name)
    print(c.cluster_id)
    print(c.spark_context_id)
    print(c.spark_version)
    print(c.node_type_id)
    print(c.driver_node_type_id)
    print(c.autoscale)
    print(c.num_workers)
    print(c.cluster_source)
    print(c.start_time)
    print(c.terminated_time)
    print(c.last_state_loss_time)
    print(c.spark_env_vars)
    print(c.spark_conf)
    print("-" * 80)
