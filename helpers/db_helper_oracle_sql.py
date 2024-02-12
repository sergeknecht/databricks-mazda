# code to get primary key of table if it exists
sql_pk_statement = """SELECT 
  tc.owner, tc.TABLE_NAME, tc.COLUMN_NAME, tc.DATA_TYPE, tc.NULLABLE, tc.NUM_NULLS, tc.NUM_DISTINCT, tc.DATA_DEFAULT, tc.AVG_COL_LEN, tc.CHAR_LENGTH,
  con.cons, ac.CONSTRAINT_NAME, ac.CONSTRAINT_TYPE, ac.STATUS, ac.INDEX_NAME
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
where  tc.owner = '{schema}' and  tc.TABLE_NAME = '{table_name}' AND ac.CONSTRAINT_TYPE = 'P'
order by 1 ,2, 3
"""