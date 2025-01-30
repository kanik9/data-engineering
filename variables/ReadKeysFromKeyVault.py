# Databricks notebook source
scopes = dbutils.secrets.listScopes()

# COMMAND ----------

for scope in scopes:
    secrets = dbutils.secrets.list(scope.name)
    print(f"Secrets in scope {scope.name}:")
    for secret in secrets:
        print(f" - {secret.key}")

# COMMAND ----------

secretKeys: dict = {
    "sql-db-username": dbutils.secrets.get(scope="keyVaultSecretManager", key="SQL-DATABASE-USER"),
    "sql-db-password": dbutils.secrets.get(scope="keyVaultSecretManager", key="SQL-DATABASE-PASSWORD"),
    "sql-db-connection-string": dbutils.secrets.get(scope="keyVaultSecretManager", key="SQL-DATABASE-JDBC-CONNECTION-STRING"),
}

# COMMAND ----------

dbutils.notebook.exit(secretKeys)

# COMMAND ----------


