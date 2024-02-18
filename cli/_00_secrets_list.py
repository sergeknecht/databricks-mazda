from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

scopes = w.dbutils.secrets.listScopes()

for s in scopes:
    print(s.name)
    for secret_metadata in w.dbutils.secrets.list(s.name):
        print('\t' + secret_metadata.key)
