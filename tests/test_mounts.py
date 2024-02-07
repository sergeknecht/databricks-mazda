# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

instance_profile = "arn:aws:iam::289450402608:instance-profile/rl-cross-databricks-nonprod"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount a bucket using an AWS instance profile

# COMMAND ----------

aws_bucket_name = "<aws-bucket-name>"
mount_name = "<mount-name>"
dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount a bucket using AWS keys

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "<aws-bucket-name>"
mount_name = "<mount-name>"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount a bucket using instance profiles with the AssumeRole policy
# MAGIC You must first configure Access cross-account S3 buckets with an AssumeRole policy.
# MAGIC
# MAGIC Mount buckets while setting S3 options in the extraConfigs:

# COMMAND ----------

dbutils.fs.mount("s3a://<s3-bucket-name>", "/mnt/<s3-bucket-name>",
  extra_configs = {
    "fs.s3a.credentialsType": "AssumeRole",
    "fs.s3a.stsAssumeRole.arn": "arn:aws:iam::<bucket-owner-acct-id>:role/MyRoleB",
    "fs.s3a.canned.acl": "BucketOwnerFullControl",
    "fs.s3a.acl.default": "BucketOwnerFullControl"
  }
)
