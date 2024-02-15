CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btnaddr AS
SELECT
  *
FROM
  impetus_ref_pii.stg.stg_emot_btnaddr;

SELECT COUNT(*) FROM impetus_poc.stg.stg_emot_btnaddr;
SELECT * FROM impetus_poc.stg.stg_emot_btnaddr;
--
CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btncustcontract AS
SELECT
  *
FROM
  impetus_ref_pii.stg.stg_emot_btncustcontract;

SELECT COUNT(*) FROM impetus_poc.stg.stg_emot_btncustcontract;
SELECT * FROM impetus_poc.stg.stg_emot_btncustcontract;
SELECT
 *
FROM
  impetus_poc.stg.stg_emot_btncustcontract
  WHERE 
  CONTRACT_ENTRY_USR <> 'MIGRATION' AND RETURN_PLATE IS NOT NULL;
--
CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btnmuser AS
SELECT
  *
FROM
  impetus_ref_pii.stg.stg_emot_btnmuser;

SELECT COUNT(*) FROM impetus_poc.stg.stg_emot_btnmuser;
SELECT * FROM impetus_poc.stg.stg_emot_btnmuser;
--
CREATE OR REPLACE TABLE impetus_poc.stg.stg_vin_emot AS
SELECT
  *
FROM
  impetus_ref_pii.stg.stg_vin_emot;

SELECT COUNT(*) FROM impetus_poc.stg.stg_vin_emot;
SELECT * FROM impetus_poc.stg.stg_vin_emot;
---------------------------------
CREATE OR REPLACE TABLE impetus_poc.stg.stg_emot_btncdhdr AS
SELECT
  *
FROM
  impetus_ref_pii.stg.stg_emot_btncdhdr;

SELECT COUNT(*) FROM impetus_poc.stg.stg_emot_btncdhdr;
SELECT * FROM impetus_poc.stg.stg_emot_btncdhdr;
------------------------------------------------------------------------------------------------------------------------------------
USE CATALOG impetus_target_pii;
USE stg;
--
CREATE OR REPLACE TABLE impetus_target.stg.stg_dim_vim AS
SELECT
  *
FROM
  impetus_target_pii.stg.stg_dim_vin;

SELECT COUNT(*) FROM impetus_target.stg.stg_dim_vim;
SELECT * FROM impetus_target.stg.stg_dim_vim;

--
CREATE OR REPLACE TABLE impetus_target.stg.stg_vin_emot AS
SELECT
  *
FROM
  impetus_target_pii.stg.stg_vin_emot;

SELECT COUNT(*) FROM impetus_target.stg.stg_vin_emot;
SELECT * FROM impetus_target.stg.stg_vin_emot;
