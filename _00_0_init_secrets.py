# Importing required libraries
from helpers.env_utils import load_env_as_list
import subprocess

accepted_scopes = ["ACC", "DEV", "PRD"]


cmd = f"databricks secrets create-scope {dev_mode}"
# Execute the bash command
subprocess.run(cmd, shell=True)

cmd = "databricks secrets list-scopes"
subprocess.run(cmd, shell=True)


secrets = load_env_as_list(dev_mode=dev_mode, env_filename=".env")
for key, value in secrets:
    cmd = f"databricks secrets put-secret {dev_mode} {key} --string-value '{value}'"
    subprocess.run(cmd, shell=True)


secret_keys = [key for key, value in secrets]
# Enumerate over secrets
for key, value in secrets.items():
    if key not in secrets:
        # Delete secrets that do not exist in secrets
        cmd = f"databricks secrets delete-secret {dev_mode} {key}"
        subprocess.run(cmd, shell=True)

cmd = f"databricks secrets list-secrets {dev_mode}"
subprocess.run(cmd, shell=True)
