SELECT "impetus_poc" , "day+1", COUNT(*) from impetus_poc.stg.STG_DSR_VEHICLE_MASTER
UNION ALL
SELECT "acc__impetus_poc" , "day+2",  COUNT(*) from acc__impetus_poc.stg.STG_DSR_VEHICLE_MASTER
UNION ALL
SELECT "dev__impetus_poc", "day+1" ,  COUNT(*) from dev__impetus_poc.stg.STG_DSR_VEHICLE_MASTER ;

select count(*) from (
select vin_cd from impetus_poc.stg.STG_DSR_VEHICLE_MASTER where vin_cd in (
select vin_cd from impetus_target.stg.IOT_STG_DSR_DLR_DIST_LKP
-- minus
-- select vin_cd from impetus_poc.staging_temp.IOT_STG_DSR_DLR_DIST_LKP_MV
)
);

SELECT count(*) from impetus_target.stg.IOT_STG_DSR_DLR_DIST_LKP;
SELECT count(*) from impetus_poc.stg.STG_DSR_VEHICLE_MASTER;


SELECT COUNT(*) FROM (

SELECT DISTINCT m.VIN_CD
FROM
impetus_poc.STG.STG_DSR_VEHICLE_MASTER m
LEFT JOIN
 impetus_poc.STG.STG_DSR_SVC_RECORDS r
 ON m.VIN_CD = r.VIN_CD
 WHERE r.VIN_CD IS NULL
);

SELECT COUNT(*) FROM (
SELECT DISTINCT r.VIN_CD
FROM impetus_poc.STG.STG_DSR_SVC_RECORDS r
LEFT JOIN
impetus_poc.STG.STG_DSR_VEHICLE_MASTER m
 ON m.VIN_CD = r.VIN_CD
 WHERE m.VIN_CD IS NULL
);
