-- dev__impetus_poc_pii --> dev__impetus_poc_pii
-- dev__impetus_target_pii  --> dev__dev__impetus_target_pii
USE CATALOG dev__impetus_poc_pii;
USE default;
CREATE
  OR REPLACE FUNCTION string_mask(attr_value STRING) RETURN CASE
    WHEN is_account_group_member('data_pii_reader_NA') THEN attr_value
    ELSE (
      CASE
        WHEN attr_value IS NULL THEN NULL
        ELSE "***"
      END
    )
  END; -- created in default schema
CREATE
  OR REPLACE FUNCTION decimal_11_2_mask(attr_value decimal(11,2)) RETURN CASE
    WHEN is_account_group_member('data_pii_reader_NA') THEN attr_value
    ELSE (
      CASE
        WHEN attr_value IS NULL THEN NULL
        ELSE 0.0
      END
    )
  END; -- created in default schema
CREATE
  OR REPLACE FUNCTION int_mask(attr_value INT) RETURN CASE
    WHEN is_account_group_member('data_pii_reader_NA') THEN attr_value
    ELSE (
      CASE
        WHEN attr_value IS NULL THEN NULL
        ELSE 0
      END
    )
  END; -- created in default schema
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER CONTACT_NAME
SET
  MASK string_mask;
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER STREET
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER STREET2
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER STREET3
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER STREET_NR
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER ZIP
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER CITY
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER PROVINCE
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER COUNTRY
SET
  MASK string_mask;
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnaddr ALTER TEL
SET
  MASK string_mask;
-------------------------
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btncustcontract ALTER RETURN_PLATE
SET
  MASK string_mask;
-------------------------
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnmuser ALTER FIRST_NAME
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnmuser ALTER MIDDLE_NAME
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btnmuser ALTER LAST_NAME
SET
  MASK string_mask;
-------------------------
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_CUST_TAGGED_USER
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_CONTACT_NAME
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET2
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET3
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET_NR
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_ZIP
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_CITY
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DOC_PROVINCE
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_CALL_OFF_USR
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_REG_NUMBER
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_poc_pii.stg.stg_vin_emot ALTER VIN_DVLA_REG_USER
SET
  MASK string_mask;
--
-------------------------
-------------------------
-------------------------
-------------------------
USE CATALOG dev__impetus_target_pii;
USE default;
CREATE
  OR REPLACE FUNCTION string_mask(attr_value STRING) RETURN CASE
    WHEN is_account_group_member('data_pii_reader_NA') THEN attr_value
    ELSE (
      CASE
        WHEN attr_value IS NULL THEN NULL
        ELSE "***"
      END
    )
  END; -- created in default schema
CREATE
  OR REPLACE FUNCTION decimal_11_2_mask(attr_value decimal(11,2)) RETURN CASE
    WHEN is_account_group_member('data_pii_reader_NA') THEN attr_value
    ELSE (
      CASE
        WHEN attr_value IS NULL THEN NULL
        ELSE 0.0
      END
    )
  END; -- created in default schema
CREATE
  OR REPLACE FUNCTION int_mask(attr_value INT) RETURN CASE
    WHEN is_account_group_member('data_pii_reader_NA') THEN attr_value
    ELSE (
      CASE
        WHEN attr_value IS NULL THEN NULL
        ELSE 0
      END
    )
  END; -- created in default schema
-------------------------
ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_SECURITY_CODE
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_CUST_TAGGED_USER
SET
  MASK string_mask;
--  ALTER TABLE
--  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_VIN_OWNER
-- SET
--  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_CUST_TAGGED_USER
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_CONTACT_NAME
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_STREET
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_STREET2
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_STREET3
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_STREET_NR
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_ZIP
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_CITY
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_DOC_PROVINCE
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_dim_vin ALTER VIN_CALL_OFF_USR
  SET
  MASK string_mask;
--
-------------------------
ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_SECURITY_CODE
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_CUST_TAGGED_USER
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_CUST_TAGGED_USER
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_CONTACT_NAME
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET2
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET3
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_STREET_NR
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_ZIP
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_CITY
SET
  MASK string_mask;
    ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_DOC_PROVINCE
SET
  MASK string_mask;
  ALTER TABLE
  dev__impetus_target_pii.stg.stg_vin_emot ALTER VIN_CALL_OFF_USR
  SET
  MASK string_mask;
-------------------------
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btncdhdr ALTER CD_NET_AMT
  SET MASK decimal_11_2_mask;
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btncdhdr ALTER CD_VAT_AMT
  SET MASK decimal_11_2_mask;
ALTER TABLE
  dev__impetus_poc_pii.stg.stg_emot_btncdhdr ALTER CD_TOT_AMT
  SET MASK decimal_11_2_mask;
--
