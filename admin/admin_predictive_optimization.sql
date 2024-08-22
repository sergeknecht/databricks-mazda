-- reauires predictive optimization to be enabled at Databricks account level
-- and unity catalog + serverless to be enabled at Databricks account + workspace level

-- Step 1: enable predictive optimization at catalog or schema level
alter database dbdemos.schema
enable predictive optimization;

-- Step 2: store history if we want to use it for monitoring
select * from system.storage.predictive_optimization_history;
