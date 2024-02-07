# Databricks notebook source
# MAGIC %md
# MAGIC # REST API Data Ingestion with PySpark
# MAGIC
# MAGIC Source= https://medium.com/@senior.eduardo92/rest-api-data-ingestion-with-pyspark-5c9c9ce89c9f

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, lit, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, MapType

# spark = SparkSession.builder.appName("REST_API_with_PySpark_DF").getOrCreate()

schema = StructType([
    StructField("length", IntegerType(), True),
    StructField("fact", StringType(), True),
    # ... Define other fields based on the API's response
])



# @udf(returnType=MapType(schema))
# @udf(returnType=MapType(StringType(), IntegerType()))
@udf(returnType=MapType(IntegerType(), StringType()))
def fetch_data(offset: int, limit: int):
    endpoint = "https://catfact.ninja/fact"
    params = {
        "offset": offset,
        "limit": limit
    }
    params = None
    response = requests.get(endpoint, params=params)
    return response.json()  # assuming API returns a list of records

# total_records = requests.get("https://catfact.ninja/fact", params={"offset": 0, "limit": 1}).json().get('total', 0)
records_per_page = 1
total_records = 100

# offsets_df = spark.range(0, total_records, records_per_page).select(col("id").alias("offset"), lit(records_per_page).alias("limit"))
# response_df = offsets_df.withColumn("response", fetch_data("offset", "limit"))
# results_df = response_df.select(explode("response"))

# COMMAND ----------

display(total_records)

# COMMAND ----------

offsets_df = spark.range(0, total_records, records_per_page).select(col("id").alias("offset"), lit(records_per_page).alias("limit"))
response_df = offsets_df.withColumn("response", fetch_data("offset", "limit"))
results_df = response_df.select(explode("response"))

# COMMAND ----------

display(response_df)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC When setting up the response schema for your specific use case, look for clues in the API provider’s documentation. Here’s an example of what the response for a GET /users API call might look like:
# MAGIC
# MAGIC ```json
# MAGIC [
# MAGIC   {
# MAGIC     "name": "Jack",
# MAGIC     "age": 30,
# MAGIC     "isActive": true
# MAGIC   },
# MAGIC   {
# MAGIC     "name": "Fred",
# MAGIC     "age": 20,
# MAGIC     "isActive": true
# MAGIC   }
# MAGIC   ...
# MAGIC ]
# MAGIC ``` 
# MAGIC
# MAGIC Based on this we would expect and array of objects, each object having three attributes: name, age, and isActive. Our schema would map these to Spark types as such:
# MAGIC
# MAGIC ```python
# MAGIC schema = StructType([
# MAGIC     StructField("name", StringType(), True),
# MAGIC     StructField("age", IntegerType(), True),
# MAGIC     StructField("isActive", BooleanType(), True),
# MAGIC     # ... Define other fields based on the API's response
# MAGIC ])
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import schema_of_json
json_data = """ {
    "fact": "Cats respond better to women than to men, probably due to the fact that women's voices have a higher pitch.",
    "length": 107
}"""
display(schema_of_json(json_data))

# COMMAND ----------

import org.apache.spark.sql.types.StructType

jsData = Seq(
  ("""{
    "name":"test","id":"12","category":[
    {
      "products":[
        "A",
        "B"
      ],
      "displayName":"test_1",
      "displayLabel":"test1"
    },
    {
      "products":[
        "C"
      ],
      "displayName":"test_2",
      "displayLabel":"test2"
    }
  ],
  "createdAt":"",
  "createdBy":""}""")
)
