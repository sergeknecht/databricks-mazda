-- Databricks notebook source
use catalog mazda_bi20_nonprd_catalog

-- COMMAND ----------

use schema session

-- COMMAND ----------

CREATE
OR REPLACE VIEW v_user as
SELECT
  current_user() as current_user,
  is_member("Finance") as is_member__finance,
  is_member("Marketing") as is_member__marketing,
  is_member("admins") as is_member__admin

-- COMMAND ----------

SELECT
  current_user() as current_user,
  is_member("Finance") as is_member__finance,
  is_member("Marketing") as is_member__marketing,
  is_member("admins") as is_member__admin
