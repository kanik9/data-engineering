# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC Read data from another notebook

# COMMAND ----------

# MAGIC %run "./CreateCustomDataFrame"

# COMMAND ----------

display(df.take(1))

# COMMAND ----------

df = df.withColumn("projects", f.split(f.col('projects'), ","))

# COMMAND ----------

df = df.withColumn("project", f.explode(f.col('projects')))

# COMMAND ----------

df = df.groupBy("emp_id").agg(f.collect_list("project").alias("projectList"))

# COMMAND ----------

display(df.take(2))

# COMMAND ----------


