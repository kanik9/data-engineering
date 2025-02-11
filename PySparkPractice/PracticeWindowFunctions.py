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

# Define window partitioned by department and ordered by salary
windowSpec = Window.partitionBy("department").orderBy(f.col("salary").desc())

# Apply window functions
df_window = df.withColumn("rank", f.rank().over(windowSpec)) \
              .withColumn("dense_rank", f.dense_rank().over(windowSpec)) \
              .withColumn("row_number", f.row_number().over(windowSpec)) \
              .withColumn("lag_salary", f.lag("salary", 1).over(windowSpec)) \
              .withColumn("lead_salary", f.lead("salary", 1).over(windowSpec)) \
              .withColumn("ntile_3", f.ntile(3).over(windowSpec))

df_window.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.window import Window

# Define the window specification using rangeBetween
windowSpecRange = Window.partitionBy('product').orderBy('unit_price').rangeBetween(Window.unboundedPreceding, Window.currentRow)

# Define the window specification using rowsBetween
windowSpecRows = Window.partitionBy('product').orderBy('unit_price').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Create a new DataFrame with the sum of total_amount using rangeBetween
df_with_range_sum = df.withColumn('range_sum', f.sum('total_amount').over(windowSpecRange))

# Create a new DataFrame with the sum of total_amount using rowsBetween
df_with_rows_sum = df.withColumn('rows_sum', f.sum('total_amount').over(windowSpecRows))

display(df_with_range_sum)
display(df_with_rows_sum)
