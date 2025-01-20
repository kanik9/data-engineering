# Databricks notebook source
# MAGIC %md
# MAGIC # Mount the ADLS Gen2 Raw data container

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://raw-datalake@etldatalakestore.blob.core.windows.net",
    mount_point = "/mnt/raw-datalake",
    extra_configs = {"fs.azure.sas.raw-datalake.etldatalakestore.blob.core.windows.net": "sp=racwdlme&st=2025-01-20T12:45:57Z&se=2026-01-20T20:45:57Z&sv=2022-11-02&sr=c&sig=viZ5wU9vtU1UQ91yecOlPHqgY%2FW1WJAKVvtmNj87Uow%3D"}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw-datalake")

# COMMAND ----------

customerDF = spark.read.table("samples.tpch.customer")
lineitemDF = spark.read.table("samples.tpch.lineitem")
nationDF = spark.read.table("samples.tpch.nation")
orderDF = spark.read.table("samples.tpch.orders")
partDF = spark.read.table("samples.tpch.part")
partsuppDF = spark.read.table("samples.tpch.partsupp")
regionDF = spark.read.table("samples.tpch.region")
suppDF = spark.read.table("samples.tpch.supplier")

# COMMAND ----------

dataframes: dict = {
    "customerDF": customerDF,
    "lineitemDF": lineitemDF,
    "nationDF": nationDF,
    "orderDF": orderDF,
    "partDF": partDF,
    "partsuppDF": partsuppDF,
    "regionDF": regionDF,
    "suppDF": suppDF
}

# COMMAND ----------

for name, df in dataframes.items():
    print(f"Dataframe {name} have total number of rows: {df.count()}")

# COMMAND ----------

customerDF.write \
    .format("delta") \
    .partitionBy("c_nationkey") \
    .mode("overwrite") \
    .save("/mnt/raw-datalake/customerDF")


# COMMAND ----------

nationDF.write\
    .format("json")\
    .mode("overwrite")\
    .save("/mnt/raw-datalake/nationDF")

# COMMAND ----------

regionDF.write\
    .format("json")\
    .mode("overwrite")\
    .save("/mnt/raw-datalake/regionDF")

# COMMAND ----------

suppDF.write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/mnt/raw-datalake/suppDF")

# COMMAND ----------

lineitemDF .write \
    .mode("overwrite") \
    .save("/mnt/raw-datalake/lineitemDF")

# COMMAND ----------

orderDF.repartition(10).write.mode("overwrite").save("/mnt/raw-datalake/orderDF")

# COMMAND ----------

partDF.write \
    .format("delta") \
    .partitionBy("p_brand") \
    .mode("overwrite") \
    .save("/mnt/raw-datalake/partDF")

# COMMAND ----------

partsuppDF.repartition(10).write.mode("overwrite").save("/mnt/raw-datalake/partsuppDF")

# COMMAND ----------


