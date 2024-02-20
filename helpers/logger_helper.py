def get_or_create_logs("scope"):



# method that accepts a dic and writes it to a databricks delta table
def log_to_delta_table(log_dict: dict):
    # create pandas dataframe from dictionary
    assert log_dict["scope"], "scope not found in log_dict"

    scope = log_dict["scope"].lower()
    df = spark.createDataFrame(data=[log_dict])
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"mle_bi_app_data.log.{scope}__application")

if __name = "__main__":
    scope = "dev"
    
    log = {
        "scope": "dev"
    }