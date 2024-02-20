from helpers.dbx_init import spark


def get_data_partitioned__by_rownum(
    db_dict: dict,
    table_name: str,  # example: "LZ_MUM.TUSER"
    bounds,
    numPartitions: int = 4,
    order_by_column: str = '',
):
    if order_by_column:
        order_by = f'order by d.{order_by_column}'
    else:
        order_by = ''

    pushdown_query = f"""(
    select ROWNUM, d.*
    from {table_name} d
    {order_by}
    ) dataset
    """

    df = (
        spark.read.format('jdbc')
        # .option("url", jdbcUrl)
        .option('dbtable', pushdown_query)
        # .option("driver", driver)
        # .option("user", username)
        # .option("password", password)
        # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
        .option('partitionColumn', 'ROWNUM')
        # lowest value to pull data for with the partitionColumn
        .option('lowerBound', f'{bounds.MIN_ID:.0f}')
        # max value to pull data for with the partitionColumn
        .option('upperBound', f'{bounds.MAX_ID+1:.0f}')
        # number of partitions to distribute the data into. Do not set this very large (~hundreds)
        .option('numPartitions', numPartitions)
        # # Oracle’s default fetchSize is 10
        # .option("fetchSize", "100")
        .options(**db_dict).load()
    )

    return df
