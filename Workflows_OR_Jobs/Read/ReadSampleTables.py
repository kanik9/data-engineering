# Databricks notebook source
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

dbutils.notebook.exit(dataframes)

# COMMAND ----------


