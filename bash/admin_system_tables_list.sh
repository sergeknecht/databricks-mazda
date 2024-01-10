#!/bin/bash

#this script will enable some databricks system tables to be visible in databricks
DBX_WS=mazdaeur-mazdaeur-mazda-bi20-nonprdvpc
DBX_META_ID="0d7058b2-94d7-4058-a8ec-14a3aa9a2ed4"
DBX_META_NAME=mazda_bi20_nonprd_catalog
#please fill in your own PAT
DBX_PAT="dapi..."

echo  "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas"

curl -v -X GET -H "Authorization: Bearer $DBX_PAT" "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas"

SCHEMA_NAME=access
curl -v -X PUT -H "Authorization: Bearer $DBX_PAT" "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas/$SCHEMA_NAME"
SCHEMA_NAME=billing
curl -v -X PUT -H "Authorization: Bearer $DBX_PAT" "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas/$SCHEMA_NAME"
SCHEMA_NAME=compute
curl -v -X PUT -H "Authorization: Bearer $DBX_PAT" "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas/$SCHEMA_NAME"
SCHEMA_NAME=marketplace
curl -v -X PUT -H "Authorization: Bearer $DBX_PAT" "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas/$SCHEMA_NAME"
SCHEMA_NAME=lineage
curl -v -X PUT -H "Authorization: Bearer $DBX_PAT" "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas/$SCHEMA_NAME"
SCHEMA_NAME=information_schema
curl -v -X PUT -H "Authorization: Bearer $DBX_PAT" "https://$DBX_WS.cloud.databricks.com/api/2.0/unity-catalog/metastores/$DBX_META_ID/systemschemas/$SCHEMA_NAME"
