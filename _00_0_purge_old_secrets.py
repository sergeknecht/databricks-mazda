from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

accepted_scopes = ["ACC", "DEV", "PRD"]

scopes = w.dbutils.secrets.listScopes()


# required_scopes = ["jdbc"]

# for scope in required_scopes:
#     if scope not in scopes:
#         w.dbutils.secrets.createScope(scope)

# # update scope list
# scopes = w.dbutils.secrets.listScopes()

for s in scopes:
    print(s.name)
    for secret_metadata in w.dbutils.secrets.list(s.name):
        print("\t" + secret_metadata.key)
