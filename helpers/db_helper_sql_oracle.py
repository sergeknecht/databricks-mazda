# code to get primary key of table if it exists
sql_pk_statement = """SELECT
  tc.owner, tc.TABLE_NAME, tc.COLUMN_NAME, tc.DATA_TYPE, tc.NULLABLE, tc.NUM_NULLS, tc.NUM_DISTINCT, tc.DATA_DEFAULT, tc.AVG_COL_LEN, tc.CHAR_LENGTH,
  con.cons, ac.CONSTRAINT_NAME, ac.INDEX_NAME
FROM DBA_TAB_COLUMNS tc
left join
  ( select  listagg( cc.constraint_name, ',') within group (order by cc.constraint_name)  cons,
         table_name, owner , column_name
         from  DBA_CONS_COLUMNS cc
          group by  table_name, owner , column_name ) con
  on con.table_name = tc.table_name and
     con.owner = tc.owner and
     con.column_name = tc.column_name
left join all_constraints ac
ON tc.owner=ac.owner and tc.TABLE_NAME=ac.TABLE_NAME AND ac.CONSTRAINT_TYPE = 'P' AND con.cons=ac.CONSTRAINT_NAME
where  tc.owner = '{schema}' and  tc.TABLE_NAME = '{table_name}' AND ac.CONSTRAINT_TYPE = 'P' AND ac.STATUS = 'ENABLED'
order by 1 ,2, 3
"""

# partitionColumn must be a numeric, date, or timestamp column from the table in the query


# code to get data type of table
# and convert it to spark data type that is compatible with it
# https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html

sql_table_schema_statement = """
SELECT
    t.owner AS schema_name,
    t.table_name,
    c.column_id,
    c.column_name,
    c.data_type,
    c.data_length,
    c.data_scale,
    c.char_length,
    c.data_default,
    CASE
    WHEN ( c.data_type = 'NUMBER'
            AND c.data_length = 22
            AND c.data_scale = 0
          )
            THEN c.column_name || ' ' || 'INT'
    WHEN  ( c.data_type = 'DATE'
            AND c.data_length = 7
            AND c.char_length = 0
          )
          THEN c.column_name || ' ' ||  'DATE'
    ELSE ''
    END DBX_DATA_TYPE,
    c.column_name DBX_COLUMN_NAME
FROM
    sys.all_tables t
    INNER JOIN sys.all_tab_columns c ON t.table_name = c.table_name
WHERE
        lower(t.owner) = lower('{schema}')
    AND lower(t.table_name) = lower('{table_name}')
ORDER BY
    t.owner,
    t.table_name,
    c.COLUMN_ID,
    c.column_name
"""

    # CASE
    #     WHEN c.data_type = 'CHAR' AND c.char_length > 1 THEN '''['' || ' || c.column_name || ' || '']'' AS ' || c.column_name
    #     ELSE c.column_name
    # END DBX_COLUMN_NAME


# AND ( ( c.data_type = 'NUMBER'
#         AND c.data_length = 22
#         AND c.data_scale = 0 )
#       OR ( c.data_type = 'DATE'
#            AND c.data_length = 7
#            AND c.char_length = 0 )
#             )
# OR ( c.data_type = 'CHAR' )

sql_top_distinct_columns_statement = """
SELECT
    t.owner AS schema_name,
    t.table_name,
    c.column_id,
    c.COLUMN_NAME,
    c.num_distinct,
    c.data_type,
    c.data_length,
    c.data_scale,
    c.char_length,
    c.data_default
FROM
         sys.all_tables t
    INNER JOIN sys.all_tab_columns c ON t.table_name = c.table_name
WHERE
        lower(t.owner) = lower('{schema}')
    AND lower(t.table_name) = lower('{table_name}')
    AND ( c.data_type = 'NUMBER'
            AND c.data_length = 22
            AND c.data_scale = 0
        )
      AND     rownum <= 5
ORDER BY
    t.owner,
    t.table_name,
    c.num_distinct DESC,
    c.COLUMN_ID,
    c.column_name
    """
