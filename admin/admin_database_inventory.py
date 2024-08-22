# Databricks notebook source
# MAGIC %md
# MAGIC # Database inventory
# MAGIC This notebook will display all tables that exist in thois workspace hive metastore and display it's owner, etc.

# COMMAND ----------

databases = spark.catalog.listDatabases()
display(databases)

# COMMAND ----------

inventory = []
# loop through tables and display database, table, and location
for db in databases:
    db_catalog = db.catalog
    db_name = db.name
    db_description = db.description
    db_location = db.locationUri
    print(f"Catalog: {db_catalog}, Database: {db_name}")

    # if db_name == "propensity_to_renew":
    #     continue

    # set the database
    spark.catalog.setCurrentDatabase(db_name)

    # get all tables
    tables = spark.sql(f"show table extended in {db_name} like '*'").collect()
    # display(tables)

    for table in tables:
        # print(table)
        database = table.database
        name = table.tableName
        is_external = "Type: EXTERNAL" in table.information
        is_managed =  "EXTERNAL" if is_external else "MANAGED" # query_base.filter("col_name = 'Is_managed_location'").select("data_type").collect()[0][0]

        try:
            query_base =  spark.sql(f"DESCRIBE EXTENDED {name}")
            owner = query_base.filter("col_name = 'Owner'").select("data_type").collect()[0][0]
            query_history_base = spark.sql(f"DESCRIBE HISTORY {name}")
            last_changed = query_history_base.select("timestamp").orderBy("timestamp", ascending=False).limit(1).collect()[0][0]
            last_changed_by = query_history_base.orderBy("timestamp", ascending=False).limit(1).select("username").collect()[0][0]
        except:
            print(f"Error while processing: {name}")

            owner = ""

        # print(f"Database: {database}, Table: {name}, Owner: {owner}, Is_Managed: {is_managed}, last_changed: {last_changed}, last_changed_by: {last_changed_by}")
        entry = {"Database":database, "Table": name, "Owner": owner, "Is_Managed": is_managed, "last_changed": last_changed, "last_changed_by": last_changed_by}
        inventory.append(entry)

display(inventory)
