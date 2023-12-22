def get_spark_driver():
    # next code is giving spark context on driver
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    sc = SparkContext('local')
    spark = SparkSession(sc)
    return spark

def get_spark_worker():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('abc').getOrCreate()
    return spark

def unity_table_exists(catalog: str, schema: str, table_name: str):
    query_txt = f"""
            SELECT 1 
            FROM {catalog}.information_schema.tables 
            WHERE table_name = '{table_name}' 
            AND table_schema='{schema}' LIMIT 1"""
    spark = get_spark_worker()
    r = spark.sql(query_txt).collect()
    return r.count() > 0

def get_fqn(catalog: str, schema: str, table_name: str) -> str:
    dbx_qualified_table_name = f"{catalog}.{schema}.{table_name}"
    return dbx_qualified_table_name