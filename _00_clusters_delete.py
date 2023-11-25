from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import State
import subprocess

w = WorkspaceClient()

for c in w.clusters.list():
    if c.state == State.TERMINATED:
        if "(clone" in c.cluster_name:  # or c.cluster_name.startswith("job-"):
            print("Deleting cluster: {}".format(c.cluster_name))
            # w.clusters.delete(c.cluster_id)
            cmd = f"databricks clusters permanent-delete {c.cluster_id}"
            subprocess.run(cmd, shell=True)
        # print("Deleting cluster: {}".format(c.cluster_name))
        # w.clusters.delete(c.cluster_id)
        continue
