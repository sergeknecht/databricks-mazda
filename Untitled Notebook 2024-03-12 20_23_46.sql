-- Databricks notebook source
-- mode "FAILFAST" aborts file parsing with a RuntimeException if malformed lines are encountered
SELECT * FROM read_files(
 -- '/Workspace/Users/sknecht@mazdaeur.com/execution_logs.tsv',
   'file:/execution_logs.tsv',
  format => 'csv',
  header => true,
  mode => 'FAILFAST',
  inferSchema => true,
  delimiter => '\t'
  ) LIMIT 100


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt")
