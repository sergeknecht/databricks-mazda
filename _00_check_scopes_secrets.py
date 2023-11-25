from databricks.sdk import WorkspaceClient
from helpers.env_utils import load_env_as_list
import subprocess

dev_scopes = ["DEV", "ACC", "PRD"]

w = WorkspaceClient()
scopes = w.dbutils.secrets.listScopes()

for scope in scopes:
    if scope.name not in dev_scopes:
        cmd = "databricks secrets delete-scope " + scope.name
        subprocess.run(cmd, shell=True)
    else:
        dev_secrets = load_env_as_list(dev_mode=scope.name)
        dev_secret_keys = [key for key, value in dev_secrets]
        secret_keys = [secret.key for secret in w.dbutils.secrets.list(scope.name)]
        for secret_key in secret_keys:
            if secret_key not in dev_secret_keys:
                # delete secret
                cmd = f"databricks secrets delete-secret {scope.name} {secret_key}"
                subprocess.run(cmd, shell=True)
        for key, value in dev_secrets:
            # update or create secret
            cmd = f"databricks secrets put-secret {scope.name} {key} --string-value '{value}'"
            subprocess.run(cmd, shell=True)

# # update scope list
scopes = w.dbutils.secrets.listScopes()
# for scope in required_scopes:
#     if scope not in scopes:
#         w.dbutils.secrets.createScope(scope)

for s in scopes:
    print(s.name)
    for secret_metadata in w.dbutils.secrets.list(s.name):
        print("\t" + secret_metadata.key)
