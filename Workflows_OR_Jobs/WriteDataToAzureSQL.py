# Databricks notebook source
# MAGIC %run ../variables/ReadKeysFromKeyVault

# COMMAND ----------


sql_db_connection_string: str = secretKeys.get(f"sql-db-connection-string")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Delta Tables into SQL 

# COMMAND ----------

# MAGIC %run ./TransformSampleTables

# COMMAND ----------

for tableName, tableData in dataframes.items():
  tableData.write\
    .jdbc(
      url=sql_db_connection_string, 
      table=tableName, 
      mode="overwrite"
      )

# COMMAND ----------


