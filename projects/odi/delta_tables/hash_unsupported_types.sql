-- plsql
-- if TABLE contains .LOB then a hash should be calculated?
-- when looking at DWH schema we only have 1 CLOB. So probably we can simply exclude this type from INGEST
-- TODO: since it probably is not used (check if important during ETL or for Exports)
WITH data AS (
    SELECT
        tc.owner,
        tc.table_name,
        tc.column_name,
        tc.data_type,
        CASE
            WHEN tc.data_precision IS NULL THEN
                0
            ELSE
                tc.data_precision
        END data_precision,
        CASE
            WHEN tc.data_scale IS NULL THEN
                0
            ELSE
                tc.data_scale
        END data_scale,
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
        ( tc.owner LIKE 'SRC_%'
          OR tc.owner LIKE 'LZ_%'
          OR tc.owner IN ( 'STG', 'STG_TMP', 'DWH', 'DWPBI' ) )
        -- AND tc.table_name = 'STG_DIM_VIN' -- AND ac.CONSTRAINT_TYPE = 'P' AND ac.STATUS IN ('ENABLED', 'VALID')
        AND data_type LIKE '%LOB'
    ORDER BY
        cons,
        num_distinct DESC,
        1,
        2,
        3
)
SELECT
    *
FROM
    data;
