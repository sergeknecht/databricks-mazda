-- Databricks notebook source
truncate

-- COMMAND ----------


CREATE OR REFRESH STREAMING LIVE  TABLE taxi_raw_test
COMMENT "LANDING_ZONE_COC.TBCOC_PRODUCT"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM cloud_files("/FileStore/tables/dlttestfile/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE taxi_raw_test_LIVE
COMMENT "LANDING_ZONE_COC.TBCOC_PRODUCT"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM cloud_files("/FileStore/tables/dltlivetableonly/", "csv",map("cloudFiles.inferColumnTypes", "true"));


-- COMMAND ----------

select * from landing_zone_coc.taxi_raw_test_live
