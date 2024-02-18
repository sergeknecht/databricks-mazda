-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Volumes Quickstart (SQL)
-- MAGIC
-- MAGIC This notebook provides an example workflow for creating your first Volume in Unity Catalog:
-- MAGIC
-- MAGIC - Choose a catalog and a schema, or create new ones.
-- MAGIC - Create a managed Volume under the chosen schema.
-- MAGIC - Browse the Volume using the three-level namespace.
-- MAGIC - Manage the Volume's access permissions.
-- MAGIC
-- MAGIC ## Requirements
-- MAGIC
-- MAGIC - Your workspace must be attached to a Unity Catalog metastore. See https://docs.databricks.com/data-governance/unity-catalog/get-started.html.
-- MAGIC - Your notebook is attached to a cluster that uses DBR 13.2+ and uses the single user or shared cluster access mode.
-- MAGIC
-- MAGIC ## Volumes under the Unity Catalog's three-level namespace
-- MAGIC
-- MAGIC Unity Catalog provides a three-level namespace for organizing data. To refer to a Volume, use the following syntax:
-- MAGIC
-- MAGIC `<catalog>.<schema>.<volume>`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Choose a catalog
-- MAGIC
-- MAGIC The following commands can help:
-- MAGIC
-- MAGIC - Show all catalogs: <a href=https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-show-catalogs.html>SHOW CATALOGS </a>.
-- MAGIC - Create a new catalog: <a href=https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-catalog.html>CREATE CATALOG</a>.
-- MAGIC - Select a catalog: <a href=https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-use-catalog.html>USE CATALOG</a>.
-- MAGIC - Show all grants on a catalog: <a href=https://docs.databricks.com/sql/language-manual/security-show-grant.html>SHOW GRANTS</a>.
-- MAGIC - Grant permissions on a catalog: <a href=https://docs.databricks.com/sql/language-manual/security-grant.html>GRANT ON CATALOG</a>.
-- MAGIC
-- MAGIC Each Unity Catalog metastore contains a default catalog named `main` with an empty schema called `default`.
-- MAGIC
-- MAGIC To create a new catalog using the `CREATE CATALOG` command, you must be a metastore admin.

-- COMMAND ----------

--- Show all catalogs in the metastore
SHOW CATALOGS;

-- COMMAND ----------

--- create a new catalog (optional)
CREATE CATALOG IF NOT EXISTS quickstart_catalog;

-- COMMAND ----------

-- Set the current catalog
USE CATALOG quickstart_catalog;

-- COMMAND ----------

--- Check grants on the quickstart catalog
SHOW GRANTS ON CATALOG quickstart_catalog;

-- COMMAND ----------

--- Make sure that all required users have USE CATALOG priviledges on the catalog.
--- In this example, we grant the priviledge to all account users.
GRANT USE CATALOG
ON CATALOG quickstart_catalog
TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Choose a schema
-- MAGIC Schemas are the second layer of the Unity Catalog namespace. They logically organize Tables, Views, Volumes and other objects.
-- MAGIC
-- MAGIC The following commands can help:
-- MAGIC
-- MAGIC - Show all schemas in a catalog: <a href=https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-show-schemas.htmlsch>SHOW SCHEMAS</a>.
-- MAGIC - Create a new schema: <a href=https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-schema.html>CREATE SCHEMA</a>.
-- MAGIC - Describe a schema: <a href=https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-describe-schema.html>DESCRIBE SCHEMA</a>.
-- MAGIC - Select a schema: <a href=https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-use-schema.html>USE SCHEMA</a>.
-- MAGIC - Show all grants on a schema: <a href=https://docs.databricks.com/sql/language-manual/security-show-grant.html>SHOW GRANTS</a>.
-- MAGIC - Grant permissions on a schema: <a href=https://docs.databricks.com/sql/language-manual/security-grant.html>GRANT ON SCHEMA</a>.

-- COMMAND ----------

-- Show schemas in the selected catalog
SHOW SCHEMAS;

-- COMMAND ----------

--- Create a new schema in the quick_start catalog
CREATE SCHEMA IF NOT EXISTS quickstart_schema
COMMENT "A new Unity Catalog schema called quickstart_schema";

-- COMMAND ----------

-- Describe a schema
DESCRIBE SCHEMA EXTENDED quickstart_schema;

-- COMMAND ----------

USE SCHEMA quickstart_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a Volume
-- MAGIC
-- MAGIC You can create `managed` or `external` volumes in Unity Catalog.
-- MAGIC
-- MAGIC We demonstrate how to create a managed volume. Please check teh documentaion for details on creating external volumes.
-- MAGIC
-- MAGIC The following commands show how to:
-- MAGIC - Grant CREATE VOLUME on a Catalog/Schema
-- MAGIC - Create a managed Volume.
-- MAGIC - Show all Volumes in a schema.
-- MAGIC - Describe a Volume.
-- MAGIC - ALTER VOLUME
-- MAGIC - DROP VOLUME
-- MAGIC - Comment on Volume

-- COMMAND ----------

--- Grant CREATE VOLUME on a Catalog or Schema.
--- When granted at Catalog level, users will be able to create Volumes on any schema in this Catalog.
GRANT CREATE VOLUME
ON CATALOG quickstart_catalog
TO `account users`;

-- COMMAND ----------

--- Create an external volume under the newly created directory
CREATE VOLUME IF NOT EXISTS quickstart_catalog.quickstart_schema.quickstart_volume
COMMENT 'This is my example managed volume'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Browse the Volume
-- MAGIC
-- MAGIC Use the path below - works with Spark APIs, shell, dbutils, and local file system utilities:
-- MAGIC
-- MAGIC `/Volumes/<catalog_name>/<schema_name>/<volume_name>/<path>`
-- MAGIC
-- MAGIC With Spark APIs you can also use:
-- MAGIC
-- MAGIC `dbfs:/Volumes/<catalog_name>/<schema_name>/<volume>/<path>`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copy a file from `dbfs:/databricks-datasets/` into your Volume.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/wine-quality/winequality-red.csv", "/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC List again and discover the file you just copied.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### List Volumes

-- COMMAND ----------

-- View all Volumes in a schema
SHOW VOLUMES IN quickstart_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Alter a Volume
-- MAGIC - Change volume name
-- MAGIC - Transfer ownership
-- MAGIC - Set a comment
-- MAGIC

-- COMMAND ----------

--- ALTER VOLUME quickstart_catalog.quickstart_schema.quickstart_volume RENAME TO quickstart_catalog.quickstart_schema.my_quickstart_volume

-- COMMAND ----------

ALTER VOLUME quickstart_catalog.quickstart_schema.quickstart_volume SET OWNER TO `account users`

-- COMMAND ----------

COMMENT ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume IS 'This is a shared Volume';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop a Volume
-- MAGIC
-- MAGIC Use the `DROP VOLUME` command to delete a volume.
-- MAGIC
-- MAGIC If a *managed volume* is dropped, the files stored in this volume are also deleted from your cloud tenant within 30 days.
-- MAGIC
-- MAGIC If an *external volume* is dropped, the metadata about the volume is removed from the catalog but the underlying files are not deleted.

-- COMMAND ----------

--- Drop the managed Volume. Uncomment the following line to try it out.
--- DROP VOLUME IF EXISTS quickstart_catalog.quickstart_schema.quickstart_volume

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manage Volume permissions
-- MAGIC
-- MAGIC Use `GRANT` and `REVOKE` statements to manage access to your Volume.
-- MAGIC
-- MAGIC #### Volume privileges
-- MAGIC The following privileges can be granted on Volume objects:
-- MAGIC - `CREATE VOLUME`: Allows the grantee to create Volumes within a catalog or schema.
-- MAGIC - `CREATE EXTERNAL VOLUME`: Applies to external locations, and allows the grantee to create an external Volume on the external location.
-- MAGIC - `READ VOLUME`: Allows the grantee to read files in a Volume.
-- MAGIC - `WRITE VOLUME`: Allows the grantee to add/remove/update files in a Volume.
-- MAGIC
-- MAGIC Like for all Unity Catalog securables, access is not automatically granted.
-- MAGIC - Initially, all users have no access.
-- MAGIC - Metastore admins and object owners can grant and revoke access to users and groups.
-- MAGIC - Granting a privilege on a securable grants the privilege on its child securables.
-- MAGIC
-- MAGIC #### Volume Ownership
-- MAGIC A principal becomes the owner of a Volume when they create it or when ownership is transferred to them by using an `ALTER` statement.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The following commands show you how to:
-- MAGIC - Grant permission on Volume
-- MAGIC - Revoke permission on Volume
-- MAGIC - Show permissions on Volume

-- COMMAND ----------

--- Lists all privileges that are granted on a Volume.
SHOW GRANTS ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume;

-- COMMAND ----------

-- Grant READ VOLUME on a volume
GRANT READ VOLUME
ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume
TO `account users`;

-- COMMAND ----------

-- Grant WRITE VOLUME on a volume
GRANT WRITE VOLUME
ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume
TO `account users`;

-- COMMAND ----------

--- Lists all privileges that are granted on a Volume.
SHOW GRANTS ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume;

-- COMMAND ----------

--- Revokes a previously granted privilege on a Volume.
REVOKE WRITE VOLUME
ON VOLUME quickstart_catalog.quickstart_schema.quickstart_volume
FROM `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Access Files in Volumes
-- MAGIC
-- MAGIC You can use dbutils, shell commands or local file system APIs to manage files stored in a Volume
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### dbutils.fs
-- MAGIC You can use any of the <a href="https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utility-dbutilsfs">dbutils file system utilities</a>, except for the mounts-related utilities.
-- MAGIC
-- MAGIC We show some examples:
-- MAGIC - create a directory inside a Volume
-- MAGIC - copy a file from a another location in this directory
-- MAGIC - list the directory and check that the file is shown inside

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/winequality-red.csv", "/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SELECT from files

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Spark Data Frame APIs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1 = spark.read.format("csv").load("dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv")
-- MAGIC display(df1)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df1 = spark.read.format("csv").load("dbfs:/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Local file system access
-- MAGIC Volumes are FUSE-mounted under `/Volumes/` on the local file system on cluster nodes.
-- MAGIC
-- MAGIC You can use shell commands and other operating system utility libraries to explore Volumes via thee FUSE mount.

-- COMMAND ----------

-- MAGIC %sh ls /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/

-- COMMAND ----------

-- MAGIC %sh echo "hi" >> /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/file.txt

-- COMMAND ----------

-- MAGIC %sh cat /Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/file.txt

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.listdir('/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC f = open('/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv', 'r')
-- MAGIC print(f.read())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC df1 = pd.read_csv("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv")

-- COMMAND ----------

-- MAGIC %r
-- MAGIC
-- MAGIC require(SparkR)
-- MAGIC
-- MAGIC df1 <- read.df("/Volumes/quickstart_catalog/quickstart_schema/quickstart_volume/destination/winequality-red.csv", "csv")
