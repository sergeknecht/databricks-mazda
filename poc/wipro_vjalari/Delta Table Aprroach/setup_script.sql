-- Databricks notebook source
create database if not exists bronze

-- COMMAND ----------


create database if not exists silver

-- COMMAND ----------


create database if not exists gold

-- COMMAND ----------

CREATE  OR REPLACE TABLE silver.STG_COC_TBCOC_PRODUCT
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
) 


-- COMMAND ----------

CREATE OR REPLACE TABLE silver.STG_COC_TBCOC_VALUE 
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
  

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.STG_EMOT_BTNCOCV (
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

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.STG_TMP_COC_ACTIVE_PRODUCTS  
          ( PRODUCTCODE  STRING 
          ) 


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df_active_product =spark.read.option('inferSchema','true').option('header','true').csv('/FileStore/tables/COC_PRODUCT.csv')
-- MAGIC display(df_active_product)
-- MAGIC df_active_product.createOrReplaceTempView('active_product')

-- COMMAND ----------

Insert into silver.STG_TMP_COC_ACTIVE_PRODUCTS 
select trim(PRODUCTCODE) from  active_product

-- COMMAND ----------

select count(1) from silver.STG_TMP_COC_ACTIVE_PRODUCTS 

-- COMMAND ----------

CREATE  OR REPLACE TABLE silver.STG_TMP_COC_TYPE_APPROVAL_HIST  
          ( PRODUCTCODE STRING,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            LINEOFFSHIFTLEVEL CHAR(1),  
            TYPE_APPROVAL STRING  
          )  

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.STG_TMP_COC_VARIANT_HIST  
          ( PRODUCTCODE STRING,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            LINEOFFSHIFTLEVEL CHAR(1),  
            VARIANT STRING
          )  

-- COMMAND ----------

CREATE  OR REPLACE TABLE silver.STG_TMP_COC_VERSION_HIST  
          ( PRODUCTCODE STRING,  
            LINEOFFBEGIN DATE,  
            LINEOFFEND DATE,  
            LINEOFFSHIFTLEVEL CHAR(1),  
            VERSION STRING 
          )  

-- COMMAND ----------

CREATE OR REPLACE TABLE	silver.IOT_DIM_COC_TYPE_APPROVAL 
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

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.STG_DIM_COC_TYPE_APPROVAL  
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

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.DIM_COC_TYPE_APPROVAL
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

-- COMMAND ----------

 select to_date('99991231', 'yyyy-MM-dd')
