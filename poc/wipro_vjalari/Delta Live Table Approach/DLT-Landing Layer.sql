-- Databricks notebook source
CREATE
OR REFRESH STREAMING LIVE TABLE lztbcoc_product (
  PRODUCTCODE STRING,
  COCNUMBER INT,
  LINEOFFBEGIN DATE,
  LINEOFFSHIFTLEVEL INT,
  LINEOFFEND DATE,
  VALUEID INT,
  STATUS INT
) --COMMENT "Raw data of tbcoc_product"
SELECT
  PRODUCTCODE,
  cast(COCNUMBER as int) as COCNUMBER,
  to_date(LINEOFFBEGIN,'dd/MM/yyyy') as LINEOFFBEGIN,
  cast(LINEOFFSHIFTLEVEL as int) as LINEOFFSHIFTLEVEL,
  to_date(LINEOFFEND,'dd/MM/yyyy') as LINEOFFEND,
  cast(VALUEID as int) as VALUEID ,
  cast(STATUS as int) as STATUS
FROM
  cloud_files(
    "s3://mazda-bi20-data-nonprd/data/Bronze_tbcoc_product/",
    "csv"
  );

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lztbcoc_value
(	DISTRIBUTOR INT,
	LANGUAGE INT,
	COCNUMBER INT,
	VALUEID INT,
	VALUE STRING,
	STATUS  INT
)
COMMENT "Raw data of tbcoc_value"
SELECT

    Cast(DISTRIBUTOR as  INT),
	Cast(LANGUAGE as  INT),
	Cast(COCNUMBER as  INT),
	Cast(VALUEID as  INT),
	VALUE ,
	Cast(STATUS  as  INT)
FROM cloud_files("s3://mazda-bi20-data-nonprd/data/Bronze_LZTBCOC_VALUE/", "csv");

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE lzbtncocv
(	VIN string,
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
COMMENT "Raw data of btncocv"
SELECT
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
    CAST(SEND_NBR as int),
    END_OF_SERIE
     FROM cloud_files("s3://mazda-bi20-data-nonprd/data/Bronze_LZBTNCOCV/", "csv");
