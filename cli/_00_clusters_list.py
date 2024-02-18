from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State

w = WorkspaceClient()
format_str = '{0: >25}'

for c in w.clusters.list():
    if c.state == State.TERMINATED:
        # print("Deleting cluster: {}".format(c.cluster_name))
        # w.clusters.delete(c.cluster_id)
        continue

    print(format_str.format('state:'), c.state, c.state_message)

    print(format_str.format('cluster_name:'), c.cluster_name)
    print(format_str.format('stacluster_idte:'), c.cluster_id)
    print(format_str.format('spark_context_id:'), c.spark_context_id)
    print(format_str.format('spark_version:'), c.spark_version)
    print(format_str.format('node_type_id:'), c.node_type_id)
    print(format_str.format('driver_node_type_id:'), c.driver_node_type_id)
    print(format_str.format('autoscale:'), c.autoscale)
    print(format_str.format('num_workers:'), c.num_workers)
    print(format_str.format('cluster_source:'), c.cluster_source)
    print(format_str.format('start_time:'), c.start_time)
    print(format_str.format('terminated_time:'), c.terminated_time)
    print(format_str.format('last_state_loss_time:'), c.last_state_loss_time)
    print(format_str.format('spark_env_vars:'), c.spark_env_vars)
    print(format_str.format('spark_conf:'), c.spark_conf)
    print('-' * 80)
