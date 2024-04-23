   ALTER SESSION
     SET CURRENT_SCHEMA = DVL12_ODI_REPO ;


--------------------------------------------------------------------------------
-- Define Variables
--------------------------------------------------------------------------------

  DEFINE cTableObject = 2400 ;
  DEFINE cPackageObject = 3200 ;
  DEFINE cLoadPlanList = "'Mazda BI', 'Mazda BI Post', 'Mazda BI Hourly'" ;


--------------------------------------------------------------------------------

    WITH TMP_FOLDER
         ( I_FOLDER,
           FOLDER_PATH
         )
      AS ( SELECT SNP_FOLDER.I_FOLDER,
                  SNP_FOLDER.FOLDER_NAME AS FOLDER_PATH
             FROM SNP_FOLDER
            WHERE SNP_FOLDER.PAR_I_FOLDER IS NULL
            ---------
            UNION ALL
            ---------
           SELECT SNP_FOLDER.I_FOLDER,
                  TMP_FOLDER.FOLDER_PATH || ' - ' || SNP_FOLDER.FOLDER_NAME AS FOLDER_PATH
             FROM TMP_FOLDER
            INNER JOIN
                  SNP_FOLDER
               ON SNP_FOLDER.PAR_I_FOLDER = TMP_FOLDER.I_FOLDER
         ),
         --------
         TMP_LP_STEP
         ( I_LP_STEP,
           LOAD_PLAN_NAME,
           SCEN_NAME,
           SCEN_VERSION
         )
      AS ( SELECT SNP_LP_STEP.I_LP_STEP,
                  SNP_LOAD_PLAN.LOAD_PLAN_NAME,
                  SNP_LP_STEP.SCEN_NAME,
                  SNP_LP_STEP.SCEN_VERSION
                  --------
             FROM SNP_LP_STEP
            INNER JOIN
                  SNP_LOAD_PLAN
               ON SNP_LOAD_PLAN.I_LOAD_PLAN = SNP_LP_STEP.I_LOAD_PLAN
                  --------
            WHERE SNP_LOAD_PLAN.LOAD_PLAN_NAME IN (&cLoadPlanList)
           -- AND SNP_LP_STEP.LP_STEP_TYPE NOT IN ('RS', 'EX')
              AND SNP_LP_STEP.PAR_I_LP_STEP IS NULL
              AND SNP_LP_STEP.IND_ENABLED = 1
            ---------
            UNION ALL
            ---------
           SELECT SNP_LP_STEP.I_LP_STEP,
                  TMP_LP_STEP.LOAD_PLAN_NAME,
                  SNP_LP_STEP.SCEN_NAME,
                  SNP_LP_STEP.SCEN_VERSION
                  --------
             FROM TMP_LP_STEP
            INNER JOIN
                  SNP_LP_STEP
               ON SNP_LP_STEP.PAR_I_LP_STEP = TMP_LP_STEP.I_LP_STEP
                  --------
            WHERE SNP_LP_STEP.IND_ENABLED = 1
         ),
         --------
         TMP_PACKAGE
      AS ( SELECT TMP_LP_SCEN.SCEN_NAME,
                  TMP_LP_SCEN.SCEN_VERSION,
                  TMP_LP_SCEN.LOAD_PLAN_LIST,
                  --------
                  SNP_PACKAGE.I_PACKAGE,
                  TMP_FOLDER.FOLDER_PATH AS PACKAGE_FOLDER,
                  SNP_PACKAGE.PACK_NAME AS PACKAGE_NAME
                  --------
             FROM ( SELECT TMP_LP_SCEN.SCEN_NAME,
                           TMP_LP_SCEN.SCEN_VERSION,
                           --------
                           LISTAGG (TMP_LP_SCEN.LOAD_PLAN_NAME, CHR (13) || CHR (10)) WITHIN GROUP (ORDER BY (1)) AS LOAD_PLAN_LIST
                           --------
                      FROM ( SELECT DISTINCT
                                    TMP_LP_STEP.SCEN_NAME,
                                    TMP_LP_STEP.SCEN_VERSION,
                                    TMP_LP_STEP.LOAD_PLAN_NAME
                                    --------
                               FROM TMP_LP_STEP
                                    --------
                              WHERE TMP_LP_STEP.SCEN_NAME IS NOT NULL
                         ) TMP_LP_SCEN
                           --------
                  GROUP BY TMP_LP_SCEN.SCEN_NAME,
                           TMP_LP_SCEN.SCEN_VERSION
                ) TMP_LP_SCEN
            INNER JOIN
                  SNP_SCEN
               ON SNP_SCEN.SCEN_NAME = TMP_LP_SCEN.SCEN_NAME
              AND SNP_SCEN.SCEN_VERSION = TMP_LP_SCEN.SCEN_VERSION
            INNER JOIN
                  SNP_PACKAGE
               ON SNP_PACKAGE.I_PACKAGE = SNP_SCEN.I_PACKAGE
            INNER JOIN
                  TMP_FOLDER
               ON TMP_FOLDER.I_FOLDER = SNP_PACKAGE.I_FOLDER
         ),
         --------
         TMP_MAPPING
         ( I_PACKAGE,
           PACKAGE_STEP,
           --------
           I_MAPPING,
           MAPPING_FOLDER,
           MAPPING_NAME,
           --------
           MAPPING_NESTING_LEVEL
         )
      AS ( SELECT SNP_STEP.I_PACKAGE,
                  SNP_STEP.NNO AS PACKAGE_STEP,
                  --------
                  SNP_MAPPING.I_MAPPING,
                  TMP_FOLDER.FOLDER_PATH AS MAPPING_FOLDER,
                  SNP_MAPPING.NAME AS MAPPING_NAME,
                  0 AS MAPPING_NESTING_LEVEL
                  --------
             FROM SNP_STEP
            INNER JOIN
                  SNP_MAPPING
               ON SNP_MAPPING.I_MAPPING = SNP_STEP.I_MAPPING
            INNER JOIN
                  TMP_FOLDER
               ON TMP_FOLDER.I_FOLDER = SNP_MAPPING.I_FOLDER
            ---------
            UNION ALL
            ---------
           SELECT TMP_MAPPING.I_PACKAGE,
                  TMP_MAPPING.PACKAGE_STEP,
                  --------
                  SNP_MAPPING.I_MAPPING,
                  TMP_FOLDER.FOLDER_PATH AS MAPPING_FOLDER,
                  SNP_MAPPING.NAME AS MAPPING_NAME,
                  TMP_MAPPING.MAPPING_NESTING_LEVEL + 1 AS MAPPING_NESTING_LEVEL
                  --------
             FROM TMP_MAPPING
            INNER JOIN
                  SNP_MAP_COMP
               ON SNP_MAP_COMP.I_OWNER_MAPPING = TMP_MAPPING.I_MAPPING
            INNER JOIN
                  SNP_MAP_REF
               ON SNP_MAP_REF.I_MAP_REF = SNP_MAP_COMP.I_MAP_REF
              AND SNP_MAP_REF.ADAPTER_INTF_TYPE = 'IReusableMapping'
            INNER JOIN
                  SNP_MAPPING
               ON SNP_MAPPING.I_MAPPING = SNP_MAP_REF.I_REF_ID
            INNER JOIN
                  TMP_FOLDER
               ON TMP_FOLDER.I_FOLDER = SNP_MAPPING.I_FOLDER
                  --------
            WHERE SNP_MAP_COMP.TYPE_NAME = 'REUSABLEMAPPING'
         ),
         --------
         TMP_MAPPING_TARGET_TABLE
      AS ( SELECT SNP_MAP_COMP.I_OWNER_MAPPING,
                  --------
                  SNP_MODEL.MOD_NAME || '.' || SNP_TABLE.RES_NAME AS TABLE_NAME
                  --------
             FROM SNP_MAP_COMP
            INNER JOIN
                  SNP_MAP_REF
               ON SNP_MAP_REF.I_MAP_REF = SNP_MAP_COMP.I_MAP_REF
              AND SNP_MAP_REF.ADAPTER_INTF_TYPE = 'IDataStore'
            INNER JOIN
                  SNP_TABLE
               ON SNP_TABLE.I_TABLE = SNP_MAP_REF.I_REF_ID
            INNER JOIN
                  SNP_MODEL
               ON SNP_MODEL.I_MOD = SNP_TABLE.I_MOD
                  --------
            WHERE SNP_MAP_COMP.TYPE_NAME = 'DATASTORE'
              AND EXISTS (
                    SELECT NULL
                      FROM SNP_MAP_CP
                     INNER JOIN
                           SNP_MAP_CONN
                        ON SNP_MAP_CONN.I_END_MAP_CP = SNP_MAP_CP.I_MAP_CP
                           --------
                     WHERE SNP_MAP_CP.I_OWNER_MAP_COMP = SNP_MAP_COMP.I_MAP_COMP
                       AND SNP_MAP_CP.DIRECTION = 'I'
                  )
         ),
         --------
         TMP_MAPPING_SOURCE_TABLES
      AS ( SELECT TMP_SOURCE_TABLE.I_OWNER_MAPPING,
                  --------
                  RTRIM (XMLAGG (XMLELEMENT (E, TMP_SOURCE_TABLE.TABLE_NAME || CHR (13) || CHR (10)) ORDER BY TMP_SOURCE_TABLE.TABLE_TYPE_ORDER, TMP_SOURCE_TABLE.TABLE_NAME).EXTRACT ('//text ()').GETCLOBVAL (), CHR (13) || CHR (10)) AS TABLE_LIST
                  --------
             FROM ( SELECT SNP_MAP_COMP.I_OWNER_MAPPING,
                           --------
                           SNP_MODEL.MOD_NAME || '.' || SNP_TABLE.RES_NAME AS TABLE_NAME,
                           CASE WHEN SNP_MODEL.COD_MOD = 'STAGING_TEMP'
                                THEN 2
                                ELSE 1
                           END AS TABLE_TYPE_ORDER
                           --------
                      FROM SNP_MAP_COMP
                     INNER JOIN
                           SNP_MAP_REF
                        ON SNP_MAP_REF.I_MAP_REF = SNP_MAP_COMP.I_MAP_REF
                       AND SNP_MAP_REF.ADAPTER_INTF_TYPE = 'IDataStore'
                     INNER JOIN
                           SNP_TABLE
                        ON SNP_TABLE.I_TABLE = SNP_MAP_REF.I_REF_ID
                     INNER JOIN
                           SNP_MODEL
                        ON SNP_MODEL.I_MOD = SNP_TABLE.I_MOD
                           --------
                     WHERE SNP_MAP_COMP.TYPE_NAME = 'DATASTORE'
                       AND NOT EXISTS (
                             SELECT NULL
                               FROM SNP_MAP_CP
                              INNER JOIN
                                    SNP_MAP_CONN
                                 ON SNP_MAP_CONN.I_END_MAP_CP = SNP_MAP_CP.I_MAP_CP
                                    --------
                              WHERE SNP_MAP_CP.I_OWNER_MAP_COMP = SNP_MAP_COMP.I_MAP_COMP
                                AND SNP_MAP_CP.DIRECTION = 'I'
                           )
                       AND EXISTS (
                             SELECT NULL
                               FROM SNP_MAP_CP
                              INNER JOIN
                                    SNP_MAP_CONN
                                 ON SNP_MAP_CONN.I_START_MAP_CP = SNP_MAP_CP.I_MAP_CP
                                    --------
                              WHERE SNP_MAP_CP.I_OWNER_MAP_COMP = SNP_MAP_COMP.I_MAP_COMP
                                AND SNP_MAP_CP.DIRECTION = 'O'
                           )
                     -----
                     UNION -- DISTINCT
                     -----
                    SELECT SNP_MAP_COMP.I_OWNER_MAPPING,
                           --------
                           'RM: ' || SNP_MAPPING.NAME AS TABLE_NAME,
                           9 AS TABLE_TYPE_ORDER
                           --------
                      FROM SNP_MAP_COMP
                     INNER JOIN
                           SNP_MAP_REF
                        ON SNP_MAP_REF.I_MAP_REF = SNP_MAP_COMP.I_MAP_REF
                       AND SNP_MAP_REF.ADAPTER_INTF_TYPE = 'IReusableMapping'
                     INNER JOIN
                           SNP_MAPPING
                        ON SNP_MAPPING.I_MAPPING = SNP_MAP_REF.I_REF_ID
                           --------
                     WHERE SNP_MAP_COMP.TYPE_NAME = 'REUSABLEMAPPING'
                ) TMP_SOURCE_TABLE
                  --------
         GROUP BY TMP_SOURCE_TABLE.I_OWNER_MAPPING
         ),
         --------
         TMP_MAPPING_COMPONENTS
      AS ( SELECT TMP_COMP.I_OWNER_MAPPING,
                  --------
                  RTRIM (XMLAGG (XMLELEMENT (E, TMP_COMP.COMP_TYPE || ': ' || TO_CHAR (TMP_COMP.COMP_COUNT, 'TM9') || CHR (13) || CHR (10)) ORDER BY TMP_COMP.COMP_TYPE).EXTRACT ('//text ()').GETCLOBVAL (), CHR (13) || CHR (10)) AS COMPONENT_LIST
                  --------
             FROM ( SELECT SNP_MAP_COMP.I_OWNER_MAPPING,
                           SNP_MAP_COMP.TYPE_NAME AS COMP_TYPE,
                           COUNT (1) AS COMP_COUNT
                           --------
                      FROM SNP_MAP_COMP
                           --------
                     WHERE SNP_MAP_COMP.TYPE_NAME NOT IN ('DATASET', 'DATASTORE', 'REUSABLEMAPPING', 'OUTPUTSIGNATURE')
                           --------
                  GROUP BY SNP_MAP_COMP.I_OWNER_MAPPING,
                           SNP_MAP_COMP.TYPE_NAME
                ) TMP_COMP
                  --------
         GROUP BY TMP_COMP.I_OWNER_MAPPING
         ),
         --------
         TMP_MAPPING_KMS
      AS ( SELECT SNP_DEPLOY_SPEC.I_OWNER_MAPPING,
                  --------
                  RTRIM (XMLAGG (XMLELEMENT (E, SNP_TRT.TRT_NAME || CHR (13) || CHR (10)) ORDER BY SNP_TRT.TRT_NAME).EXTRACT ('//text ()').GETCLOBVAL (), CHR (13) || CHR (10)) AS KM_LIST
                  --------
             FROM SNP_DEPLOY_SPEC
            INNER JOIN
                  SNP_PHY_NODE
               ON SNP_PHY_NODE.I_OWNER_DS = SNP_DEPLOY_SPEC.I_DEPLOY_SPEC
            INNER JOIN
                  SNP_MAP_REF
                  SNP_MAP_REF
               ON SNP_MAP_REF.I_MAP_REF = NVL (SNP_PHY_NODE.I_SRC_COMP_KM, SNP_PHY_NODE.I_TGT_COMP_KM)
            INNER JOIN
                  SNP_TRT
               ON SNP_TRT.I_TRT = SNP_MAP_REF.I_REF_ID
                  --------
            WHERE SNP_TRT.TRT_NAME NOT LIKE 'XKM%'
                  --------
         GROUP BY SNP_DEPLOY_SPEC.I_OWNER_MAPPING
         ),
         --------
         TMP_PROCEDURE
      AS ( SELECT SNP_STEP.I_PACKAGE,
                  SNP_STEP.NNO AS PACKAGE_STEP,
                  --------
                  SNP_TRT.I_TRT AS I_PROCEDURE,
                  TMP_FOLDER.FOLDER_PATH AS PROEDURE_FOLDER,
                  SNP_TRT.TRT_NAME AS PROCEDURE_NAME
                  --------
             FROM SNP_STEP
            INNER JOIN
                  SNP_TRT
               ON SNP_TRT.I_TRT = SNP_STEP.I_TRT
            INNER JOIN
                  TMP_FOLDER
               ON TMP_FOLDER.I_FOLDER = SNP_TRT.I_FOLDER
         ),
         --------
         TMP_ACTION
      AS ( SELECT SNP_STEP.I_PACKAGE,
                  SNP_STEP.NNO AS PACKAGE_STEP,
                  --------
                  SNP_STEP.I_TXT_ACTION,
                  CAST (SUBSTR (SNP_TXT_HEADER.FULL_TEXT, 1, 400) AS VARCHAR2 (400)) AS ACTION_TXT
                  --------
             FROM SNP_STEP
            INNER JOIN
                  SNP_TXT_HEADER
               ON SNP_TXT_HEADER.I_TXT = SNP_STEP.I_TXT_ACTION
         )
         --------
  SELECT TMP_PACKAGE.LOAD_PLAN_LIST,
         TMP_PACKAGE.SCEN_NAME,
         --------
         TMP_PACKAGE.PACKAGE_FOLDER,
         TMP_PACKAGE.PACKAGE_NAME,
         TMP_MAPPING.PACKAGE_STEP,
         --------
         CASE WHEN TMP_MAPPING.MAPPING_NESTING_LEVEL > 0
              THEN 'REUSABLE MAPPING'
              ELSE 'MAPPING'
         END AS OBJECT_TYPE,
         TMP_MAPPING.MAPPING_FOLDER AS OBJECT_FOLDER,
         TMP_MAPPING.MAPPING_NAME AS OBJECT_NAME,
         TMP_MAPPING.MAPPING_NESTING_LEVEL AS OBJECT_NESTING_LEVEL,
         --------
         TMP_MAPPING_TARGET_TABLE.TABLE_NAME AS TARGET_TABLE_NAME,
         TMP_MAPPING_SOURCE_TABLES.TABLE_LIST AS SOURCE_TABLE_LIST,
         TMP_MAPPING_COMPONENTS.COMPONENT_LIST,
         TMP_MAPPING_KMS.KM_LIST
         --------
    FROM TMP_PACKAGE
   INNER JOIN
         TMP_MAPPING
      ON TMP_MAPPING.I_PACKAGE = TMP_PACKAGE.I_PACKAGE
    LEFT OUTER JOIN
         TMP_MAPPING_TARGET_TABLE
      ON TMP_MAPPING_TARGET_TABLE.I_OWNER_MAPPING = TMP_MAPPING.I_MAPPING
    LEFT OUTER JOIN
         TMP_MAPPING_SOURCE_TABLES
      ON TMP_MAPPING_SOURCE_TABLES.I_OWNER_MAPPING = TMP_MAPPING.I_MAPPING
    LEFT OUTER JOIN
         TMP_MAPPING_COMPONENTS
      ON TMP_MAPPING_COMPONENTS.I_OWNER_MAPPING = TMP_MAPPING.I_MAPPING
    LEFT OUTER JOIN
         TMP_MAPPING_KMS
      ON TMP_MAPPING_KMS.I_OWNER_MAPPING = TMP_MAPPING.I_MAPPING
   ---------
   UNION ALL
   ---------
  SELECT TMP_PACKAGE.LOAD_PLAN_LIST,
         TMP_PACKAGE.SCEN_NAME,
         --------
         TMP_PACKAGE.PACKAGE_FOLDER,
         TMP_PACKAGE.PACKAGE_NAME,
         TMP_PROCEDURE.PACKAGE_STEP,
         --------
         'PROCEDURE' AS OBJECT_TYPE,
         TMP_PROCEDURE.PROEDURE_FOLDER AS OBJECT_FOLDER,
         TMP_PROCEDURE.PROCEDURE_NAME AS OBJECT_NAME,
         NULL AS OBJECT_NESTING_LEVEL,
         --------
         NULL AS TARGET_TABLE_NAME,
         NULL AS SOURCE_TABLE_LIST,
         NULL AS COMPONENT_LIST,
         NULL AS KM_LIST
         --------
    FROM TMP_PACKAGE
   INNER JOIN
         TMP_PROCEDURE
      ON TMP_PROCEDURE.I_PACKAGE = TMP_PACKAGE.I_PACKAGE
   ---------
   UNION ALL
   ---------
  SELECT TMP_PACKAGE.LOAD_PLAN_LIST,
         TMP_PACKAGE.SCEN_NAME,
         --------
         TMP_PACKAGE.PACKAGE_FOLDER,
         TMP_PACKAGE.PACKAGE_NAME,
         TMP_ACTION.PACKAGE_STEP,
         --------
         'ODI TOOL' AS OBJECT_TYPE,
         NULL AS OBJECT_FOLDER,
         TMP_ACTION.ACTION_TXT AS OBJECT_NAME,
         NULL AS OBJECT_NESTING_LEVEL,
         --------
         NULL AS TARGET_TABLE_NAME,
         NULL AS SOURCE_TABLE_LIST,
         NULL AS COMPONENT_LIST,
         NULL AS KM_LIST
         --------
    FROM TMP_PACKAGE
   INNER JOIN
         TMP_ACTION
      ON TMP_ACTION.I_PACKAGE = TMP_PACKAGE.I_PACKAGE
         --------
ORDER BY PACKAGE_FOLDER,
         PACKAGE_NAME,
         PACKAGE_STEP,
         OBJECT_NESTING_LEVEL ;
