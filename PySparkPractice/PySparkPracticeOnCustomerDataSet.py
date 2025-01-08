# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    IntegerType, 
    FloatType, 
    LongType, 
    DoubleType, 
    TimestampType
)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datastore/practice2/StoresDataSheet/")

# COMMAND ----------

customer_schema: StructType = StructType([
    StructField("customer_id", IntegerType()),
    StructField("f_name", StringType()),
    StructField("l_name", StringType()),
    StructField("email", StringType()),
])

# COMMAND ----------

product_schema: StructType = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("supplier_id", IntegerType()),
]) 

# COMMAND ----------

customer_df = spark.read.format("csv").option("header", "true").schema(customer_schema).load("/mnt/raw_datastore/practice2/customers.csv")
product_df = spark.read.format("csv").option("header", "true").schema(product_schema).load("/mnt/raw_datastore/practice2/products.csv")

# COMMAND ----------

product_df.head(1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Option 1: Using Wildcard (*.csv)
# MAGIC In this approach, you're using a wildcard (*.csv) to load all CSV files in a folder at once. This means Spark will automatically scan the entire directory and load all CSV files that match the pattern.
# MAGIC   - Pros:
# MAGIC       - Efficiency: This is typically much faster because Spark can optimize the loading of files. It reads all files in parallel, especially in a distributed environment.
# MAGIC       - Parallel Processing: Spark will automatically distribute the reading of CSV files across the available executors, which is especially useful for large datasets.
# MAGIC       - Simplicity: You don't need to manually specify each file. The code is cleaner and shorter.
# MAGIC       - Scalability: If you add more CSV files to the directory, the code will automatically adapt without requiring changes. This is ideal if the number of CSV files in the directory might grow over time.
# MAGIC   - Cons:
# MAGIC       - InferSchema Overhead: When inferSchema is enabled, Spark has to read the entire file to infer the types of columns. This can be slow for large datasets. However, in most real-world scenarios, this overhead is acceptable and outweighed by the benefits of automatic parallelization and simplicity.
# MAGIC ###Option 2: Read Files Separately and Union Them
# MAGIC     In this approach, you're reading each CSV file separately and using union() to combine them.
# MAGIC
# MAGIC   - Pros:
# MAGIC     - Fine-Grained Control: You have full control over which files are being read, and you can explicitly handle each one differently (e.g., apply custom transformations to different files before combining them).
# MAGIC     - Schema Flexibility: If each file has a different schema, you could adjust the schema manually for each file (although in your case, all files seem to have the same schema).
# MAGIC   - Cons:
# MAGIC     - Inefficiency:
# MAGIC         - Repeated File I/O: Each file is read sequentially, and the data is loaded into separate DataFrames before being unioned. This results in unnecessary file I/O operations, which can be slower for large numbers of files.
# MAGIC         - Union Performance: The union() operation is not a trivial operation in PySpark. Each call to union() requires a shuffle of data, and when you chain multiple union() calls, the performance degrades significantly due to repeated shuffling.
# MAGIC         - Single File Processing: Each file is processed individually, which doesn’t take advantage of Spark's parallelization capabilities as efficiently as in Option 1.
# MAGIC     - Code Complexity:
# MAGIC         - Hard to Scale: This approach becomes cumbersome if you have many files to read, as you will need to explicitly list each file and manually union them. This is less maintainable, especially if the list of files changes or grows over time.

# COMMAND ----------

# store1_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/raw_datastore/practice2/StoresDataSheet/store_101.csv")
# store2_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/raw_datastore/practice2/StoresDataSheet/store_102.csv")
# store3_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/raw_datastore/practice2/StoresDataSheet/stores_103.csv")

# COMMAND ----------

# store_df = store1_df.union(store2_df).union(store3_df)

# COMMAND ----------

# store_df.count()

# COMMAND ----------

stors_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/raw_datastore/practice2/StoresDataSheet/*.csv")

# COMMAND ----------

{column : stors_df.filter(f.col(f"{column}").isNull()).count() for column in stors_df.columns}

# COMMAND ----------

stors_df = stors_df.fillna(0)

# COMMAND ----------

stors_df.head(5)

# COMMAND ----------

temp_detailed_df = stors_df.join(
    f.broadcast(product_df), 
    on=product_df.product_id == stors_df.ProductID, 
    how="inner"
    )

# COMMAND ----------

display(temp_detailed_df.head(5))

# COMMAND ----------

temp_detailed_df = temp_detailed_df.withColumn("TotalAmount", (f.col("QuantitySold")*f.col("price")))

# COMMAND ----------

display(temp_detailed_df.head(5))

# COMMAND ----------

temp_detailed_df = temp_detailed_df.withColumn(
    "DiscountAmount", f.round(f.lit(
        (f.col("QuantitySold")*f.col("price"))*(f.col("DiscountPercentage")/100)
        ))
    )

# COMMAND ----------

display(temp_detailed_df.head(5))

# COMMAND ----------

temp_detailed_df = temp_detailed_df.withColumn("BillingAmount", f.col("TotalAmount") - f.col("DiscountAmount"))

# COMMAND ----------

display(temp_detailed_df.head(5))

# COMMAND ----------

temp_detailed_df.groupBy("CustomerID").agg(f.sum("BillingAmount")).show()

# COMMAND ----------

temp_detailed_df.groupBy(["TransactionID", "CustomerID"]).agg(f.count("QuantitySold")).show()

# COMMAND ----------


