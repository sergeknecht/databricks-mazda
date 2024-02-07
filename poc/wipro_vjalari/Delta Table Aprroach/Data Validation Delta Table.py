# Databricks notebook source
# MAGIC %sql
# MAGIC --select count(1) from bronze.lzbtncocv
# MAGIC --select count(1) from bronze.lztbcoc_product
# MAGIC select count(1) from bronze.lztbcoc_value

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(1) from silver.stg_coc_tbcoc_product; --12120415
# MAGIC --select count(1) from silver.stg_coc_tbcoc_value;  --621747
# MAGIC --select count(1) from silver.stg_tmp_coc_active_products --7006
# MAGIC --select count(1) from silver.stg_tmp_coc_type_approval_hist  --34905
# MAGIC --select count(1) from silver.stg_tmp_coc_variant_hist --7068
# MAGIC --select count(1) from silver.stg_tmp_coc_version_hist --7646
# MAGIC --select count(1) from silver.iot_dim_coc_type_approval --356728
# MAGIC --select count(1) from silver.stg_dim_coc_type_approval --41939
# MAGIC --select count(1) from gold.dim_coc_type_approval --41939

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_coc_type_approval
