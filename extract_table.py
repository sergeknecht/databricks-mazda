# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import json
import sys
import time
import traceback

from helpers.db_helper import (
    get_bounds__by_rownum,
    get_connection_properties__by_key,
    get_db_dict,
    get_jdbc_data_by_dict,
)
from helpers.db_helper_delta import table_exists
from helpers.db_helper_oracle import get_data_partitioned__by_rownum
from helpers.db_helper_oracle_sql import sql_pk_statement
from helpers.status_helper import create_status

# COMMAND ----------

# dbutils.widgets.dropdown(
#     "p_scope", "ACC", choices=["ACC", "PRD"], label="Development Scope"
# )
# dbutils.widgets.text(
#     "p_catalog_name_target", "impetus_ref", label="Catalog Name Target"
# )
# dbutils.widgets.text("p_schema_name_source", "STG", label="Schema Name Source")
# dbutils.widgets.text(
#     "p_table_name_source", "STG_LEM_TRANSPORT_MODES", label="Table Name Source"
# )
# dbutils.widgets.text(
#     "p_db_key", "DWH_BI1", label="Database Key"
# )
dbutils.widgets.text('p_work_json', '{}', label='Database Table Extract JSON Config')

# COMMAND ----------

start_time = time.time()

p_work_json: dict = json.loads(dbutils.widgets.get('p_work_json'))
assert p_work_json, 'p_work_json not set'

p_scope = p_work_json['scope']
p_db_key = p_work_json['db_key']
p_catalog_name_source = p_work_json['catalog_name_source']
p_schema_name_source = p_work_json['schema_name_source']
p_table_name_source = p_work_json['table_name_source']
p_catalog_name_target = p_work_json['catalog_name_target']
p_schema_name_target = p_work_json['schema_name_target']
p_table_name_target = p_work_json['table_name_target']
p_mode = p_work_json.get('mode', 'overwrite')

dbx_qualified_table_name = (
    f'{p_catalog_name_target}.{p_schema_name_target}.{p_table_name_target}'
)
print(dbx_qualified_table_name)
print(p_mode)

# COMMAND ----------

db_conn_props: dict = get_connection_properties__by_key(p_scope, p_db_key)
work_item = p_work_json | {
    'db_conn_props': db_conn_props,
}
# merge p_work_json with db_conn_props (=overwrite)

# COMMAND ----------

class DelayedResult:
    def __init__(self, work_item: dict):
        self.exc_info = None
        self.result = None
        self.catalog_name_source = work_item['catalog_name_source']
        self.schema_name_source = work_item['schema_name_source']
        self.table_name_source = work_item['table_name_source']
        self.catalog_name_target = work_item['catalog_name_target']
        self.schema_name_target = work_item['schema_name_target']
        self.table_name_target = work_item['table_name_target']
        self.query_type = work_item['query_type']
        self.query_sql = work_item['query_sql']
        self.scope = work_item['scope']
        self.db_conn_props = work_item['db_conn_props']
        self.work_item = work_item
        self.mode = work_item['mode']

        self.fqn = f'{self.catalog_name_target}.{self.schema_name_target}.{self.table_name_target}'

    def test_exception(self):
        try:
            raise Exception('This is a test exception')
        except Exception as e:
            self.exc_info = (e, traceback.format_exc())

    def do_work(self):
        try:
            # create the target schema if it does not exist
            spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name_target}.{self.schema_name_target} WITH DBPROPERTIES (Scope='{self.scope}')"
            )

            # get all primary keys and indexes so that we can also apply it in the target table
            pk_sql = sql_pk_statement.format(
                **{
                    'schema': self.schema_name_source,
                    'table_name': self.table_name_source,
                }
            )
            df_pk = get_jdbc_data_by_dict(
                db_conn_props=db_conn_props,
                work_item={
                    'query_sql': pk_sql,
                    'query_type': 'query',
                },
            )

            # get the data
            import pprint as pp

            pp.pprint(self.work_item)
            df = get_jdbc_data_by_dict(
                db_conn_props=db_conn_props,
                work_item={
                    **self.work_item,
                    'table_sql': f'{self.schema_name_source}.{self.table_name_source}',
                },
            )

            if df.count() == 0:
                result = create_status(
                    status_code=404, status_message=f'NO_DATA: {self.fqn}'
                )
                result['fqn'] = self.fqn
                result['row_count'] = df.count()
                result['work_item'] = {
                    **self.work_item,
                    'table_sql': f'{self.schema_name_source}.{self.table_name_source}',
                }
                self.result = result
                return

            df.write.format('delta').mode(self.mode).saveAsTable(self.fqn)

            if self.mode == 'overwrite':
                for row_pk in df_pk.collect():
                    column_name = row_pk['COLUMN_NAME']
                    sqls = [
                        f'ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET NOT NULL',
                        f'ALTER TABLE {self.fqn} DROP PRIMARY KEY IF EXISTS CASCADE',
                        f'ALTER TABLE {self.fqn} ADD CONSTRAINT pk_{table_name}_{column_name} PRIMARY KEY({column_name})',
                        f"ALTER TABLE {self.fqn} ALTER COLUMN {column_name} SET TAGS ('db_schema' = 'pk')",
                    ]
                    for curr_sql in sqls:
                        # print("\t", curr_sql)
                        spark.sql(curr_sql)

            result = create_status(status_code=201, status_message=f'CREATED: {self.fqn}')
            result['fqn'] = self.fqn
            result['row_count'] = df.count()
            self.result = result
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            tb = traceback.TracebackException(exc_type, exc_value, exc_tb)
            stack_trace = traceback.format_exc(limit=2, chain=True)
            if 'JVM stacktrace' in stack_trace:
                stack_trace = stack_trace.split('JVM stacktrace:')[0]
            status_message = ''.join(tb.format_exception_only())
            if 'JVM stacktrace' in status_message:
                status_message = status_message.split('JVM stacktrace:')[0]
            self.exc_info = (
                status_message,
                stack_trace,
            )

    def get_result(self):
        end_time = time.time()
        time_duration = int(end_time - start_time)
        # print(f"Execution time: {execution_time} seconds")
        if self.exc_info:
            # Rethrow the exception using sys.exc_info()
            e, traceback = self.exc_info
            # _, _, traceback = exc_info

            result = create_status(status_code=500, status_message='ERROR:' + e)
            result['fqn'] = self.fqn
            result['traceback'] = traceback
            result['time_duration'] = time_duration
            return json.dumps(result)

            # raise e.with_traceback(traceback)
        # raise self.exc_info[1], None, self.exc_info[2]
        self.result['time_duration'] = time_duration
        return json.dumps(self.result)


# COMMAND ----------

if (
    not table_exists(p_catalog_name_target, p_schema_name_target, p_table_name_target)
    or p_mode == 'append'
):

    # try:
    dr = DelayedResult(work_item=work_item)
    dr.do_work()

    dbutils.notebook.exit(dr.get_result())
else:
    result = create_status(
        status_code=208, status_message=f'SKIPPED: {dbx_qualified_table_name}'
    )
    # 208: already reported
    result['fqn'] = dbx_qualified_table_name
    end_time = time.time()
    time_duration = int(end_time - start_time)
    result['time_duration'] = time_duration
    dbutils.notebook.exit(json.dumps(result))
