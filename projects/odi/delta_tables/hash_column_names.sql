
set long 100000;
set longchunksize 100000;
set lines 10000;

set newpage 0;
set echo off;
set feedback off;
set heading off;

-- example script: get a list of all column names from a table
-- TODO: exclude LONG, and clob/blob types columns
WITH data AS (
    SELECT
        tc.owner,
        tc.table_name,
        tc.column_name,
        tc.identity_column,
        tc.data_type,
        tc.nullable,
        ac.constraint_type,
        ac.status,
        tc.num_nulls,
        tc.num_distinct,
        tc.data_default,
        tc.avg_col_len,
        tc.char_length,
        con.cons,
        ac.constraint_name,
        ac.index_name
    FROM
        all_tab_columns tc
        LEFT JOIN (
            SELECT
                LISTAGG(cc.constraint_name, ',') WITHIN GROUP(
                ORDER BY
                    cc.constraint_name
                ) cons,
                table_name,
                owner,
                column_name
            FROM
                all_cons_columns cc
            GROUP BY
                table_name,
                owner,
                column_name
        )               con ON con.table_name = tc.table_name
                 AND con.owner = tc.owner
                 AND con.column_name = tc.column_name
        LEFT JOIN all_constraints ac ON tc.owner = ac.owner
                                        AND tc.table_name = ac.table_name
                                        AND ac.constraint_type = 'P'
                                        AND con.cons = ac.constraint_name
    WHERE
            tc.owner = 'DWH'
            AND tc.data_type NOT LIKE '%LOB'
     --   AND tc.table_name = 'DIM_VIN' -- AND ac.CONSTRAINT_TYPE = 'P' AND ac.STATUS IN ('ENABLED', 'VALID')
    ORDER BY
        1,
        2,
        3
) -- , COLS as (
SELECT
    d.owner || '.' ||
    d.table_name || ': ' ||
    RTRIM(XMLAGG(XMLELEMENT(E,d.column_name, ' || ').EXTRACT('//text()') ORDER BY d.owner, d.table_name).GetClobVal(),',') AS LIST
--    LISTAGG(d.column_name, ' || ') WITHIN GROUP(
--        ORDER BY
--            d.owner, d.table_name
--        ) column_name
FROM
    data d
GROUP BY
    d.owner ,
    d.table_name;
    -- ,floor((ROWNUM + 1) / 100))

--SELECT  c.owner,
--    c.table_name,
--    RTRIM(XMLAGG(XMLELEMENT(E,c.column_name, ' || ').EXTRACT('//text()') ORDER BY c.owner, c.table_name).GetClobVal(),',') AS LIST
--FROM COLS c
--GROUP BY
--    c.owner,
--    c.table_name;
