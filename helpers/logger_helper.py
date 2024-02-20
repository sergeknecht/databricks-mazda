CATALOG = "mle_bi_app_data"
SCHEMA = "log"
TABLE_APPLICATION = "{scope}__application"
FQN = "mle_bi_app_data.log.{scope}__application"

# method that accepts a dic and writes it to a databricks delta table
def log_to_delta_table(log_dict: dict):
    # create pandas dataframe from dictionary
    assert log_dict["scope"], "scope not found in log_dict"

    scope = log_dict["scope"].lower()
    log_dict["user_name"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

    df = spark.createDataFrame(data=[log_dict])
    # write to delta table
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(FQN.format(scope=scope))

if __name__ == "__main__":
    scope = "dev"

    log = {
        "scope": "dev"
    }
