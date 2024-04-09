# Mazda - Databricks Ingestion

## Database Connection

Connection is done using:

- config: `config\db_configs.json`
- secrets - can be listed using: `cli/_00_secrets_list.py`
  - ACC
    - DWH_BI1__JDBC_DRIVER (not used)
    - DWH_BI1__JDBC_PASSWORD
    - DWH_BI1__JDBC_URL (not used)
    - DWH_BI1__JDBC_USERNAME
  - PRD
    - DWH_BI1__JDBC_PASSWORD
    - DWH_BI1__JDBC_USERNAME
