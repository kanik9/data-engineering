# Databricks notebook source
# MAGIC %md
# MAGIC ##PySpark Basic Operation Practice

# COMMAND ----------

from pyspark.sql.types import (
  StructType, 
  StringType, 
  IntegerType, 
  StructField, 
  LongType, 
  DateType, 
  IntegerType, 
  ShortType, 
  BooleanType, 
  ByteType, 
  FloatType
  )

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount datastore

# COMMAND ----------

try: 
  dbutils.fs.mount(
  source="wasbs://raw-datastore@etldatalakepoc.blob.core.windows.net",
  mount_point="/mnt/raw_datastore",
  extra_configs = {
    'fs.azure.account.key.etldatalakepoc.blob.core.windows.net': dbutils.secrets.get("databricksAllUsersScope", "storageAccountKey")
  }
)
except Exception as error:
  # print(error)
  pass

# COMMAND ----------

display(dbutils.fs.ls("/mnt/raw_datastore/practice/"))

# COMMAND ----------

df_custom_schema: StructType = StructType([
  StructField('row_id',  LongType()),
  StructField("timestamp", DateType()),
  StructField("user_id", IntegerType()),
  StructField("content_id", ShortType()),
  StructField("content_type_id", IntegerType()),
  StructField("task_container_id", ShortType()),
  StructField("user_answer", ByteType()),
  StructField("answered_correctly", ByteType()),
  StructField("prior_question_elapsed_time", FloatType()),
  StructField("prior_question_had_explanation", IntegerType())
])

# COMMAND ----------

# df = spark.read.format("parquet").option("header", "true").schema(df_custom_schema).load("/mnt/raw_datastore/practice/")
df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load("/mnt/raw_datastore/practice/")

# COMMAND ----------

df = df.repartition(10, 'user_id')

# COMMAND ----------

display(df.printSchema())

# COMMAND ----------

df.count()

# COMMAND ----------

{column : df.filter(f.col(f"{column}").isNull()).count() for column in df.columns}

# COMMAND ----------

display(df.head(10))

# COMMAND ----------

{column: df.select(f"{column}").distinct().count() for column in df.columns}

# COMMAND ----------


