from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for c in w.clusters.list():
    print(c.cluster_name)
    print(c.cluster_id)
    print(c.spark_context_id)
    print(c.spark_version)
    print(c.node_type_id)
    print(c.driver_node_type_id)
    print(c.autoscale)
    print(c.cluster_source)
    print(c.state)
    print(c.state_message)
    print(c.start_time)
    print(c.terminated_time)
    print(c.last_state_loss_time)
    print(c.spark_env_vars)
    print(c.spark_conf)
    print("-" * 80)
