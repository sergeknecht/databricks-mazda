-- Databricks notebook source
--select count(1) from mazda_bi20_nonprd_catalog.bronze.lztbcoc_product --12120415
--select count(1) from mazda_bi20_nonprd_catalog.bronze.lztbcoc_value  --621747
select count(1) from mazda_bi20_nonprd_catalog.bronze.lzbtncocv --4606

-- COMMAND ----------

--select count(1) from mazda_bi20_nonprd_catalog.silver.stg_coc_tbcoc_product --12120415
--select count(1) from mazda_bi20_nonprd_catalog.silver.stg_coc_tbcoc_value  --621740
 --select count(1) from mazda_bi20_nonprd_catalog.silver.stg_emot_btncocv --4606
 ---select count(1) from mazda_bi20_nonprd_catalog.silver.stg_tmp_coc_active_products --7006
 --select count(1) from mazda_bi20_nonprd_catalog.silver.stg_tmp_coc_type_approval_hist --34905 
 --select count(1) from mazda_bi20_nonprd_catalog.silver.stg_tmp_coc_variant_hist --7068
--select count(1) from mazda_bi20_nonprd_catalog.silver.stg_tmp_coc_version_hist --7646
--select count(1) from mazda_bi20_nonprd_catalog.silver.iot_dim_coc_type_approval --356728
select count(1) from mazda_bi20_nonprd_catalog.silver.stg_dim_coc_type_approval --41939

-- COMMAND ----------

select * from mazda_bi20_nonprd_catalog.silver.DIM_COC_TYPE_APPROVAL
