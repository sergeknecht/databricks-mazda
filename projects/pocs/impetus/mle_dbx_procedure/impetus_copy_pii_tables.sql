-- Databricks notebook source

USE CATALOG acc__impetus_poc_pii;

-- COMMAND ----------

USE stg;

-- COMMAND ----------

CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btnaddr AS
SELECT
  *
FROM
 acc__impetus_poc_pii.stg.stg_emot_btnaddr;

-- COMMAND ----------

SELECT * FROM impetus_poc.stg.stg_emot_btnaddr;
--

-- COMMAND ----------

CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btncustcontract AS
SELECT
  *
FROM
 acc__impetus_poc_pii.stg.stg_emot_btncustcontract;

-- COMMAND ----------

--
SELECT
 *
FROM
  impetus_poc.stg.stg_emot_btncustcontract
  WHERE
  CONTRACT_ENTRY_USR <> 'MIGRATION' AND RETURN_PLATE IS NOT NULL;

-- COMMAND ----------

--
CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btnmuser AS
SELECT
  *
FROM
 acc__impetus_poc_pii.stg.stg_emot_btnmuser;


-- COMMAND ----------

SELECT * FROM impetus_poc.stg.stg_emot_btnmuser;
--

-- COMMAND ----------

CREATE OR REPLACE TABLE impetus_poc.stg.stg_vin_emot AS
SELECT
  *
FROM
 acc__impetus_poc_pii.stg.stg_vin_emot;

-- COMMAND ----------

SELECT * FROM impetus_poc.stg.stg_vin_emot;
---------------------------------

-- COMMAND ----------

CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btncdhdr AS
SELECT
  *
FROM
 acc__impetus_poc_pii.stg.stg_emot_btncdhdr;

-- COMMAND ----------

SELECT * FROM impetus_poc.stg.stg_emot_btncdhdr;

-- COMMAND ----------

------------------------------------------------------------------------------------------------------------------------------------

-- COMMAND ----------

USE CATALOG acc__impetus_target_pii;

-- COMMAND ----------

USE stg;

-- COMMAND ----------

--
CREATE OR REPLACE TABLE impetus_target.stg.stg_dim_vim AS
SELECT
  *
FROM
 acc__impetus_target_pii.stg.stg_dim_vin;

-- COMMAND ----------

SELECT * FROM impetus_target.stg.stg_dim_vim;

-- COMMAND ----------

--
CREATE OR REPLACE TABLE impetus_target.stg.stg_vin_emot AS
SELECT
  *
FROM
 acc__impetus_target_pii.stg.stg_vin_emot;

-- COMMAND ----------

SELECT * FROM impetus_target.stg.stg_vin_emot;
