-- Databricks notebook source
CREATE LIVE TABLE STG_COC_TBCOC_PRODUCT
( 
	PRODUCTCODE STRING,
	COCNUMBER INT,
	LINEOFFBEGIN DATE,
	LINEOFFSHIFTLEVEL INT,
	LINEOFFEND DATE,
	VALUEID INT,
	STATUS INT, 
	CREATE_USER STRING,
	CREATE_TS DATE,
	UPDATE_USER STRING,
	UPDATE_TS DATE,
	ETL_JOB_NAME STRING,
	SOURCE STRING,
	LOAD_NR STRING 
) COMMENT "Raw data of STG_COC_TBCOC_PRODUCT"
select DISTINCT
  PRODUCTCODE,
  COCNUMBER,
  LINEOFFBEGIN,
  LINEOFFSHIFTLEVEL,
  LINEOFFEND,
  VALUEID,
  STATUS,
  'DWSRV' as CREATE_USER,
  cast(getdate() as date) as CREATE_TS,
  'DWSRV' as UPDATE_USER,
  cast(getdate() as date) as UPDATE_TS,
  'COC  -  TIA  -  STG_COC_TBCOC_PRODUCT' as ETL_JOB_NAME,
  'C_SOURCE_COC' as SOURCE,
  'V_DWH_LOAD_12C_LOAD_NR' as LOAD_NR
from
mazda_bi20_nonprd_catalog.bronze.lztbcoc_product

-- COMMAND ----------

CREATE LIVE TABLE STG_COC_TBCOC_VALUE 
( 
	DISTRIBUTOR INT,
	LANGUAGE INT,
	COCNUMBER INT,
	VALUEID INT,
	VALUE STRING,
	STATUS  INT,
	CREATE_USER STRING,
	CREATE_TS DATE,
	UPDATE_USER STRING,
	UPDATE_TS DATE,
	ETL_JOB_NAME STRING,
	SOURCE STRING,
	LOAD_NR STRING 
) 
select DISTINCT
  DISTRIBUTOR,
  LANGUAGE,
  COCNUMBER,
  VALUEID,
  VALUE,
  STATUS,
  'DWSRV' as CREATE_USER,
  cast(getdate() as date) as CREATE_TS,
  'DWSRV' as UPDATE_USER,
  cast(getdate() as date) as UPDATE_TS,
  'COC  -  TIA  -  STG_COC_TBCOC_VALUE' as ETL_JOB_NAME,
  'C_SOURCE_COC' as SOURCE,
  'V_DWH_LOAD_12C_LOAD_NR' as LOAD_NR
FROM
mazda_bi20_nonprd_catalog.bronze.lztbcoc_value

-- COMMAND ----------

CREATE LIVE TABLE STG_EMOT_BTNCOCV (
  VIN string,
DISTCD string,
MODEL string,
SPEC_CODE string,
EXT_COL_CD string,
INT_COL_CD string,
FCT_CLAS string,
BSC string,
LINE_OFF_DT string,
COC_DT string,
NSC_ID string,
L_UPD_TST string,
L_UPD_USR string,
SEND_FLG string,
SEND_TST string,
SEND_NBR int,
END_OF_SERIE string
)
SELECT DISTINCT
  VIN,
  DISTCD,
  MODEL,
  SPEC_CODE,
  EXT_COL_CD,
  INT_COL_CD,
  FCT_CLAS,
  BSC,
  LINE_OFF_DT,
  COC_DT,
  NSC_ID,
  L_UPD_TST,
  L_UPD_USR,
  SEND_FLG,
  SEND_TST,
  SEND_NBR,
  END_OF_SERIE
FROM
  mazda_bi20_nonprd_catalog.bronze.lzbtncocv

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE STG_TMP_COC_ACTIVE_PRODUCTS
(
  PRODUCTCODE  STRING 
)
AS SELECT distinct trim(PRODUCTCODE) as PRODUCTCODE  FROM cloud_files("/FileStore/tables/", "csv") 
WHERE PRODUCTCODE IS NOT NULL

-- COMMAND ----------

CREATE LIVE TABLE STG_TMP_COC_TYPE_APPROVAL_HIST  
          ( PRODUCTCODE STRING,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            LINEOFFSHIFTLEVEL STRING,  
            TYPE_APPROVAL STRING  
          ) 
          (SELECT
  SP.PRODUCTCODE AS PRODUCTCODE,
  SP.LINEOFFBEGIN AS LINEOFFBEGIN,
  CAST (
    LEAST (
      SP.LINEOFFEND,
      (
        LEAD (SP.LINEOFFBEGIN - 1, 1, SP.LINEOFFEND) OVER (
          PARTITION BY SP.PRODUCTCODE,
          SP.LINEOFFSHIFTLEVEL
          ORDER BY
            SP.LINEOFFBEGIN
        )
      )
    ) AS DATE
  ) AS LINEOFFEND,
  CAST(SP.LINEOFFSHIFTLEVEL AS CHAR(1)) AS LINEOFFSHIFTLEVEL,
  SV.VALUE AS TYPE_APPROVAL
FROM
  LIVE.stg_coc_tbcoc_product SP
  INNER JOIN LIVE.stg_coc_tbcoc_value SV ON SP.COCNUMBER = SV.COCNUMBER
  AND SP.VALUEID = SV.VALUEID
  INNER JOIN LIVE.stg_tmp_coc_active_products SAP ON SAP.PRODUCTCODE = trim(SP.PRODUCTCODE)
WHERE
  (
    SV.COCNUMBER = 10
    AND SV.DISTRIBUTOR = 0
    AND SV.LANGUAGE = 1
    AND SV.STATUS = 2
  ) 
          )

-- COMMAND ----------


CREATE LIVE TABLE STG_TMP_COC_VARIANT_HIST
    ( PRODUCTCODE STRING,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            LINEOFFSHIFTLEVEL STRING,  
            VARIANT STRING
    )  
SELECT
  SP.PRODUCTCODE AS PRODUCTCODE,
  SP.LINEOFFBEGIN AS LINEOFFBEGIN,
  SP.LINEOFFEND AS LINEOFFEND,
  CAST (SP.LINEOFFSHIFTLEVEL AS CHAR(1)) AS LINEOFFSHIFTLEVEL,
  SV.VALUE AS VARIANT
FROM
  LIVE.stg_coc_tbcoc_product SP
  INNER JOIN LIVE.stg_tmp_coc_active_products SAP ON SAP.PRODUCTCODE = trim(SP.PRODUCTCODE)
  INNER JOIN LIVE.stg_coc_tbcoc_value SV ON SP.COCNUMBER = SV.COCNUMBER
  AND SP.VALUEID = SV.VALUEID
WHERE
  (
    SV.COCNUMBER = 3
    AND SV.DISTRIBUTOR = 0
    AND SV.LANGUAGE = 1
    AND SV.STATUS = 2
  )

-- COMMAND ----------

CREATE LIVE TABLE STG_TMP_COC_VERSION_HIST 
    ( PRODUCTCODE STRING,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            LINEOFFSHIFTLEVEL STRING,  
            VERSION STRING 
          )  

SELECT
  SP.PRODUCTCODE AS PRODUCTCODE,
  SP.LINEOFFBEGIN AS LINEOFFBEGIN,
  SP.LINEOFFEND AS LINEOFFEND,
  CAST (SP.LINEOFFSHIFTLEVEL AS CHAR(1)) AS LINEOFFSHIFTLEVEL,
  SV.VALUE AS VERSION
FROM
  LIVE.stg_coc_tbcoc_product SP
  INNER JOIN LIVE.stg_tmp_coc_active_products SAP ON SAP.PRODUCTCODE = trim(SP.PRODUCTCODE)
  INNER JOIN LIVE.stg_coc_tbcoc_value SV ON SP.COCNUMBER = SV.COCNUMBER
  AND SP.VALUEID = SV.VALUEID
WHERE
  (
    SV.COCNUMBER = 4
    AND SV.DISTRIBUTOR = 0
    AND SV.LANGUAGE = 1
    AND SV.STATUS = 2
  )

-- COMMAND ----------

CREATE LIVE TABLE IOT_DIM_COC_TYPE_APPROVAL 
( 
	PRODUCTCODE STRING,
	LINEOFFBEGIN DATE,
	LINEOFFEND DATE, 
	LINEOFFSHIFTLEVEL INT ,
  TYPE_APPROVAL_SK INT,
	CREATE_USER STRING,
	CREATE_TS TIMESTAMP,
	UPDATE_USER STRING,
	UPDATE_TS TIMESTAMP,
	ETL_JOB_NAME STRING,
	SOURCE STRING,
	LOAD_NR STRING
) 
SELECT
  DISTINCT BP.PRODUCTCODE,
  BP.LINEOFFBEGIN,
  lead(
    BP.LINEOFFBEGIN - 1,
    1,
    to_date('99991231', 'yyyyMMdd')
  ) over (
    partition by BP.PRODUCTCODE,
    BP.LINEOFFSHIFTLEVEL
    order by
      BP.LINEOFFBEGIN
  ) LINEOFFEND,
  BP.LINEOFFSHIFTLEVEL,
  null as TYPE_APPROVAL_SK,
	'DWSRV' as CREATE_USER,
	getdate() as CREATE_TS,
	'DWSRV' as UPDATE_USER,
	getdate() as UPDATE_TS,
	'COC  -  TBCOC_PRODUCT  -  CTAS  -  IOT_DIM_COC_TYPE_APPROVAL' as ETL_JOB_NAME,
	'C_SOURCE_COC' as SOURCE,
	'V_DWH_LOAD_12C_LOAD_NR' as LOAD_NR 
FROM
    mazda_bi20_nonprd_catalog.bronze.lztbcoc_product BP
WHERE
  (
    BP.COCNUMBER in (10, 3, 4)
    AND BP.STATUS = 2
  )

-- COMMAND ----------


CREATE LIVE TABLE STG_DIM_COC_TYPE_APPROVAL  
              ( TYPE_APPROVAL_SK INT,  
            TYPE_APPROVAL STRING,  
            VARIANT STRING,  
            VERSION STRING,  
            PRODUCTCODE STRING,  
            LINEOFFSHIFTLEVEL INT,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            CREATE_USER STRING,  
            CREATE_TS DATE,  
            UPDATE_USER STRING,  
            UPDATE_TS DATE,  
            ETL_JOB_NAME STRING,  
            SOURCE STRING,  
            LOAD_NR INT  
          )  
SELECT
  IOTAP.TYPE_APPROVAL_SK AS TYPE_APPROVAL_SK,
  trim(APH.TYPE_APPROVAL) AS TYPE_APPROVAL,
  trim(VAH.VARIANT) AS VARIANT,
  trim(VSH.VERSION) AS VERSION,
  trim(IOTAP.PRODUCTCODE) AS PRODUCTCODE,
  IOTAP.LINEOFFSHIFTLEVEL AS  LINEOFFSHIFTLEVEL,
  IOTAP.LINEOFFBEGIN AS  LINEOFFBEGIN,
  IOTAP.LINEOFFEND AS LINEOFFEND,
  'DWSRV' AS CREATE_USER,  
  cast(getdate() as date)  AS CREATE_TS,  
  'DWSRV' AS UPDATE_USER,  
  cast(getdate() AS DATE) AS UPDATE_TS,  
  'LOOKUP_SK  -  CTAS  -  STG_DIM_COC_TYPE_APPROVAL' AS ETL_JOB_NAME,  
  '#MAZDA__BI.C_SOURCE_COC' AS SOURCE,  
  null AS LOAD_NR   
FROM
  LIVE.IOT_DIM_COC_TYPE_APPROVAL IOTAP
  INNER JOIN LIVE.STG_TMP_COC_VERSION_HIST VSH
  ON IOTAP.PRODUCTCODE = VSH.PRODUCTCODE
  AND IOTAP.LINEOFFSHIFTLEVEL = VSH.LINEOFFSHIFTLEVEL
  AND IOTAP.LINEOFFBEGIN BETWEEN VSH.LINEOFFBEGIN AND VSH.LINEOFFEND
  INNER JOIN  LIVE.STG_TMP_COC_VARIANT_HIST VAH
  ON  IOTAP.PRODUCTCODE = VAH.PRODUCTCODE
  AND IOTAP.LINEOFFSHIFTLEVEL = VAH.LINEOFFSHIFTLEVEL
  AND IOTAP.LINEOFFBEGIN BETWEEN VAH.LINEOFFBEGIN AND VAH.LINEOFFEND
  INNER JOIN LIVE.STG_TMP_COC_TYPE_APPROVAL_HIST APH
  ON IOTAP.PRODUCTCODE = APH.PRODUCTCODE
    AND IOTAP.LINEOFFSHIFTLEVEL = APH.LINEOFFSHIFTLEVEL
    AND IOTAP.LINEOFFBEGIN BETWEEN APH.LINEOFFBEGIN AND APH.LINEOFFEND
  

-- COMMAND ----------

CREATE LIVE TABLE DIM_COC_TYPE_APPROVAL
 ( TYPE_APPROVAL_SK INT,  
            TYPE_APPROVAL STRING,  
            VARIANT STRING,  
            VERSION STRING,  
            PRODUCTCODE STRING,  
            LINEOFFSHIFTLEVEL INT,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            CREATE_USER STRING,  
            CREATE_TS DATE,  
            UPDATE_USER STRING,  
            UPDATE_TS DATE,  
            ETL_JOB_NAME STRING,  
            SOURCE STRING,  
            LOAD_NR INT  
          )  
SELECT * FROM LIVE.STG_DIM_COC_TYPE_APPROVAL
