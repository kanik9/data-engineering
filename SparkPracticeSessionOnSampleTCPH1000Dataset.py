# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.types import *
# from graphframes import GraphFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data first 

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

# MAGIC %md
# MAGIC ## Data Basic Infromation 

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
  print(f"Total number of default partitions for {name} are", df.rdd.getNumPartitions())

# COMMAND ----------

for name, df in dataframes.items():
  print(f"Total number of rows for {name} are", df.count())

# COMMAND ----------

customerDF.printSchema()
print("#"*50)
print(f"Total Number of rows are: {customerDF.count()}")
print("#"*50)
val = {}
for column in customerDF.columns:
    val[column] = [
      {
      f"distinct_count": customerDF.select(f.col(column)).distinct().count(),
      f"null_count": customerDF.filter(f.col(column).isNull()).count()
      }
    ]
display(val)


# COMMAND ----------

print("With default number of partitions")
lineitemDF.printSchema()
print("#"*50)
print(f"Total Number of rows are: {lineitemDF.count()}")
print("#"*50)
val = {}
for column in lineitemDF.columns:
    val[column] = [
      {
      f"distinct_count": lineitemDF.select(f.col(column)).distinct().count(),
      f"null_count": lineitemDF.filter(f.col(column).isNull()).count()
      }
    ]
display(val)

# COMMAND ----------

lineitemDF_5_partition = lineitemDF.coalesce(5)

# COMMAND ----------

lineitemDF_5_partition.rdd.getNumPartitions()

# COMMAND ----------

# print("With 5 number of partitions")
# lineitemDF_5_partition.printSchema()
# print("#"*50)
# print(f"Total Number of rows are: {lineitemDF_5_partition.count()}")
# print("#"*50)
# val = {}
# for column in lineitemDF_5_partition.columns:
#     val[column] = [
#       {
#       f"distinct_count": lineitemDF_5_partition.select(f.col(column)).distinct().count(),
#       f"null_count": lineitemDF_5_partition.filter(f.col(column).isNull()).count()
#       }
#     ]
# display(val)

# COMMAND ----------

# for df in [customerDF, lineitemDF, nationDF, orderDF, partDF, partsuppDF, regionDF, suppDF]:
#   # print(f"Basic Infromation for {df.name} table is: ")
#   print(f"Total Number of rows are: {df.count()}")
#   display(df.describe())

# COMMAND ----------

dataframes.keys()

# COMMAND ----------

for df_name in ['nationDF', 'orderDF', 'partDF', 'partsuppDF', 'regionDF', 'suppDF']:
  print(f"Dataframe '{df_name}' With default number of partitions")
  df = dataframes[df_name]
  df.printSchema()
  print("#"*50)
  print(f"Total Number of rows are: {df.count()}")
  print("#"*50)
  val = {}
  for column in df.columns:
      val[column] = [
        {
        f"distinct_count": df.select(f.col(column)).distinct().count(),
        f"null_count": df.filter(f.col(column).isNull()).count()
        }
      ]
  display(val)

# COMMAND ----------

# Top 10 customers by account balance 
display(
  customerDF.select(
    [
      'c_name', 
      'c_mktsegment', 
      'c_address', 
      'c_phone', 
      'c_acctbal'
    ]
  ).orderBy('c_acctbal', ascending=False).limit(10)
)

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

a = customerDF.groupBy('c_mktsegment').agg(
    _sum('c_acctbal').alias('total_account_balance')
)
a.orderBy('total_account_balance', ascending=False).show()

# COMMAND ----------

customerDF.createOrReplaceTempView("customer_table")

# COMMAND ----------

spark.sql("select c_mktsegment as mktsegment, sum(c_acctbal) as total_account_balance from customer_table group by c_mktsegment  order by total_account_balance DEsc").show()

# COMMAND ----------

# DBTITLE 1,Create a new column | round of the amount to 0
customerDF = customerDF.withColumn('rounded_acctbal', f.round('c_acctbal', 0))
display(customerDF)


# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

custom_window = Window.partitionBy('c_mktsegment').orderBy(f.desc('rounded_acctbal'))

# Apply the window functions
result_df = customerDF.select(
    'c_name', 
    'c_mktsegment', 
    'c_address', 
    'c_phone', 
    'c_acctbal',
    'rounded_acctbal',
    f.dense_rank().over(custom_window).alias('dense_rank'), 
    f.rank().over(custom_window).alias('rank')
)

display(result_df)

# COMMAND ----------

# Display the first 10 results
display(result_df.select("*").groupBy('c_mktsegment').min('dense_rank').limit(10))

# COMMAND ----------

# DBTITLE 1,Highest ranked customer in each department based on the amount\
# Display the first 10 results
display(result_df.groupBy('c_mktsegment').agg(
  f.first('c_name').alias("customer_name"),
  f.first('c_address').alias("address"),
  f.first('c_phone').alias("phone"),
  f.first('c_acctbal').alias("acctbal"),
  f.first('rounded_acctbal').alias("rounded_acctbal"),
  f.min('dense_rank').alias("rank"),
  f.first('rank').alias("dense_rank")
  ).orderBy('c_mktsegment')
)

# COMMAND ----------

display(lineitemDF_5_partition.take(5))

# COMMAND ----------

customerDF.groupby("c_mktsegment").agg(
  f.first('c_name').alias("customer_name"),
  f.first('c_address').alias("address"),
  f.first('c_phone').alias("phone"),
  f.max('c_acctbal')
  ).show()

# COMMAND ----------

display(customerDF.orderBy("c_acctbal", ascending=False).limit(1))

# COMMAND ----------

customerDF.select(f.max("c_acctbal")).show()

# COMMAND ----------

# DBTITLE 1,Handle few columns
lineitemDF_5_partition = lineitemDF_5_partition.withColumn("l_quantity_new", f.col("l_quantity").cast("int"))
lineitemDF_5_partition = lineitemDF_5_partition.withColumn("l_extendedprice_new", f.round(f.col("l_extendedprice"), 2))
lineitemDF_5_partition = lineitemDF_5_partition.withColumn("l_discount_new", (f.round(f.col("l_discount"), 2) * 100).cast("int"))
lineitemDF_5_partition = lineitemDF_5_partition.withColumn("l_tax_new", f.round(f.col("l_tax"), 2))

# COMMAND ----------

lineitemDF_5_partition.printSchema()

# COMMAND ----------

lineitemDF_5_partition.head(10)

# COMMAND ----------

lineitemDF.head(10)

# COMMAND ----------

lineitemDF.take(10)

# COMMAND ----------

lineitemDF_5_partition.take(10)

# COMMAND ----------

# DBTITLE 1,Orders with Discount greater than 10%
display(
  lineitemDF_5_partition.filter(f.col("l_discount_new") >= 10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the Schema

# COMMAND ----------

customer_to_nation_df_withoutbroadcast = customerDF.join(
  nationDF, 
  on=[customerDF.c_nationkey == nationDF.n_nationkey],
  how='inner'
).join(
  regionDF,
  on=[nationDF.n_regionkey == regionDF.r_regionkey],
  how='inner'
)

# COMMAND ----------

display(customer_to_nation_df_withoutbroadcast.head(10))

# COMMAND ----------

customer_to_nation_df = customerDF.join(
  f.broadcast(nationDF), 
  on=[customerDF.c_nationkey == nationDF.n_nationkey],
  how='inner'
).join(
  f.broadcast(regionDF),
  on=[nationDF.n_regionkey == regionDF.r_regionkey],
  how='inner'
).select(
  customerDF.c_custkey.alias("customer_key"),
  nationDF.n_nationkey.alias("nation_key"),
  regionDF.r_regionkey.alias("region_key"),
  customerDF.c_name.alias("customer_name"),
  customerDF.c_address.alias("customer_address"),
  customerDF.c_phone.alias("customer_phone"),
  f.round(customerDF.c_acctbal, 2).alias("customer_acctbal"),
  customerDF.c_mktsegment.alias("mktsegment"),
  regionDF.r_name.alias("region_name"),
  nationDF.n_name.alias("nation_name"),
  regionDF.r_comment.alias("region_comment"),
  nationDF.n_comment.alias("nation_comment"),
  customerDF.c_comment.alias("customer_comment"),
)

# COMMAND ----------

customer_to_nation_df.cache()

# COMMAND ----------

customer_to_nation_df.is_cached

# COMMAND ----------

display(customer_to_nation_df.head(10))

# COMMAND ----------

display(customer_to_nation_df.groupBy("region_name", "nation_name", "mktsegment").agg(
    f.sum(
        f.round(
            f.col('customer_acctbal'), 2
        )
    ).alias("Total_account_balance"),
))

# COMMAND ----------

display(
  customer_to_nation_df.groupBy("region_name", "nation_name", "mktsegment").agg(
    f.sum(
        f.round(
            f.col('customer_acctbal'), 2
        )
    ).alias("Total_account_balance"),
  ).orderBy("Total_account_balance", ascending=False)
)

# COMMAND ----------

display(
  customer_to_nation_df.groupBy("region_name").agg(
    f.count(f.col('customer_key')).alias("Total_customers")
  ).orderBy("Total_customers", ascending=False)
)

# COMMAND ----------

customer_to_nation_df.count()

# COMMAND ----------

display(orderDF.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC - Total number of rows for customerDF are 750000
# MAGIC - Total number of rows for lineitemDF are 29999795
# MAGIC - Total number of rows for nationDF are 25
# MAGIC - Total number of rows for orderDF are 7500000
# MAGIC - Total number of rows for partDF are 1000000
# MAGIC - Total number of rows for partsuppDF are 4000000
# MAGIC - Total number of rows for regionDF are 5
# MAGIC - Total number of rows for suppDF are 50000

# COMMAND ----------

# DBTITLE 1,Create a max view of order table
# max_view_order_table = orderDF.join(
#   f.broadcast(customer_to_nation_df),
#   on=[customer_to_nation_df.customer_key == orderDF.o_custkey],
#   how='inner'
# ).join(
#   lineitemDF,
#   on=[orderDF.o_orderkey == lineitemDF.l_orderkey],
#   how='inner'
# ).join(
#   partDF,
#   on=[lineitemDF.l_partkey == partDF.p_partkey],
#   how='inner'
# ).join(
#   f.broadcast(suppDF),
#   on=[lineitemDF.l_suppkey == suppDF.s_suppkey],
#   how='inner'
# )

# COMMAND ----------

# display(max_view_order_table.head(2))

# COMMAND ----------

# max_view_order_table1 = orderDF.join(
#   customer_to_nation_df,
#   on=[customer_to_nation_df.customer_key == orderDF.o_custkey],
#   how='inner'
# ).join(
#   lineitemDF,
#   on=[orderDF.o_orderkey == lineitemDF.l_orderkey],
#   how='inner'
# ).join(
#   partDF,
#   on=[lineitemDF.l_partkey == partDF.p_partkey],
#   how='inner'
# ).join(
#   f.broadcast(suppDF),
#   on=[lineitemDF.l_suppkey == suppDF.s_suppkey],
#   how='inner'
# )

# COMMAND ----------

# display(max_view_order_table1.head(2))

# COMMAND ----------

# max_view_order_table2 = orderDF.join(
#   customer_to_nation_df,
#   on=[customer_to_nation_df.customer_key == orderDF.o_custkey],
#   how='inner'
# ).join(
#   lineitemDF,
#   on=[orderDF.o_orderkey == lineitemDF.l_orderkey],
#   how='inner'
# ).join(
#   partDF,
#   on=[lineitemDF.l_partkey == partDF.p_partkey],
#   how='inner'
# ).join(
#   f.broadcast(suppDF),
#   on=[lineitemDF.l_suppkey == suppDF.s_suppkey],
#   how='inner'
# )

# COMMAND ----------

# display(max_view_order_table2.head(2))

# COMMAND ----------

# max_view_order_table2.explain(extended=True)

# COMMAND ----------

customer_to_nation_df.count()

# COMMAND ----------

max_view_order_table = orderDF.join(
  customer_to_nation_df,
  on=[customer_to_nation_df.customer_key == orderDF.o_custkey],
  how='inner'
).join(
  lineitemDF,
  on=[orderDF.o_orderkey == lineitemDF.l_orderkey],
  how='inner'
).join(
  partDF,
  on=[lineitemDF.l_partkey == partDF.p_partkey],
  how='inner'
).join(
  f.broadcast(suppDF),
  on=[lineitemDF.l_suppkey == suppDF.s_suppkey],
  how='inner'
).select(
  # All Keys 
  customer_to_nation_df.customer_key,
  customer_to_nation_df.nation_key,
  customer_to_nation_df.region_key,
  orderDF.o_orderkey.alias("order_key"),
  partDF.p_partkey.alias("part_key"),
  suppDF.s_suppkey.alias("supplier_key"),
  customer_to_nation_df.customer_name,
  customer_to_nation_df.customer_address,
  customer_to_nation_df.customer_phone,
  customer_to_nation_df.customer_acctbal,
  customer_to_nation_df.mktsegment,
  customer_to_nation_df.region_name,
  customer_to_nation_df.nation_name,
  orderDF.o_orderstatus.alias("order_status"),
  f.round(orderDF.o_totalprice, 2).alias("order_billing_amount"),
  orderDF.o_orderdate.alias("order_date"),
  orderDF.o_orderpriority.alias("order_priority"),
  orderDF.o_clerk.alias("order_clerk"),
  orderDF.o_shippriority.alias("order_ship_priority"),
  orderDF.o_comment.alias("order_comment"),
  lineitemDF.l_linenumber.alias("order_line_item_number"),
  lineitemDF.l_quantity.alias("order_line_item_quantity"),
  f.round(lineitemDF.l_extendedprice, 2).alias("order_line_item_extended_price"),
  f.cast("int", f.round((lineitemDF.l_discount)*100, 2)).alias("order_line_item_discount"),
  f.cast("int", f.round((lineitemDF.l_tax) * 100, 2)).alias("order_line_item_tax"),
  lineitemDF.l_returnflag.alias("order_line_item_return_flag"),
  lineitemDF.l_shipdate.alias("order_line_item_ship_date"),
  lineitemDF.l_commitdate.alias("order_line_item_commit_date"),
  lineitemDF.l_shipmode.alias("order_line_item_ship_mode"),
  partDF.p_name.alias("part_name"),
  partDF.p_mfgr.alias("part_manufacturer"),
  partDF.p_brand.alias("part_brand"),
  partDF.p_type.alias("part_type"),
  partDF.p_size.alias("part_size"),
  partDF.p_container.alias("part_container"),
  partDF.p_retailprice.alias("part_retail_price"),
  suppDF.s_name.alias("supplier_name"),
  suppDF.s_address.alias("supplier_address"),
  suppDF.s_phone.alias("supplier_phone"),
  suppDF.s_acctbal.alias("supplier_acctbal"),
  # All type of comments 
  customer_to_nation_df.region_comment,
  customer_to_nation_df.nation_comment,
  customer_to_nation_df.customer_comment,
  lineitemDF.l_comment.alias("order_line_item_comment"),
  suppDF.s_comment.alias("supplier_comment"),
  partDF.p_comment.alias("part_comment"),
)

# COMMAND ----------

max_view_order_table.take(10)

# COMMAND ----------

display(max_view_order_table.head(2))

# COMMAND ----------

# max_view_order_table = None
# max_view_order_table1 = None
# max_view_order_table2 = None

# COMMAND ----------

customer_to_nation_df.unpersist()

# COMMAND ----------

max_view_order_table.count()

# COMMAND ----------

max_view_order_table.cache()

# COMMAND ----------

max_view_order_table.columns

# COMMAND ----------

#  rename the dataframe
# max_view_order_table = max_view_order_table3

# COMMAND ----------

max_view_order_table.createOrReplaceTempView("max_view_order_table")

# COMMAND ----------

# DBTITLE 1,Total Sales Revenue
display(max_view_order_table.agg(f.sum("order_billing_amount").alias("sum_order_billing_amount")))

# display(spark.sql(
#   "select sum(order_billing_amount) from max_view_order_table"
# ))

# COMMAND ----------

# DBTITLE 1,Average Order Value
display(
  max_view_order_table.agg(
    f.avg("order_billing_amount").alias("avg_order_billing_amount")
  )
)

# COMMAND ----------

display(max_view_order_table.groupBy("order_key").agg(f.sum("order_billing_amount")))

# COMMAND ----------

# DBTITLE 1,On-Time Delivery Rate
# order_line_item_ship_date <= order_line_item_commitdate.
display(
  max_view_order_table.filter(
    f.col('order_line_item_ship_date') <= f.col('order_line_item_commit_date')
    )
)

# COMMAND ----------

# DBTITLE 1,Top Selling Products
top_selling_products = max_view_order_table.groupBy('part_name').agg(
  f.first("region_name").alias("region_name"),
  f.sum("order_line_item_quantity").alias("total_quantity"),
  f.sum('order_line_item_extended_price').alias("total_price")
)

# COMMAND ----------

top_selling_products.display()

# COMMAND ----------

cust_window = Window().partitionBy("customer_name", "region_name").orderBy(f.desc("total_price"))

# COMMAND ----------

# DBTITLE 1,Identify the top 5 customers by total order value in each region
top_5_cust_orders = max_view_order_table.groupby(
  ["customer_name", "region_name"]
  ).agg(
    f.first("nation_name").alias('nation'),
    f.first("mktsegment").alias("market_segment"),
    f.round(f.sum('order_billing_amount'), 2).alias("total_price"),
  )

top_5_cust_orders = top_5_cust_orders.withColumn("rank", f.dense_rank().over(cust_window))
top_5_cust_orders = top_5_cust_orders.filter(f.col("rank") <= 5)

# COMMAND ----------

display(top_5_cust_orders)

# COMMAND ----------

# DBTITLE 1,Calculate the average discount given on orders for each part category.
avg_discount_by_part_category = max_view_order_table.groupby(
  "part_type"
).agg(
  f.round(f.avg('order_line_item_discount'), 0).alias("avg_discount")
).orderBy(f.desc("avg_discount"))
display(avg_discount_by_part_category.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Sure! Here are some problem statements similar to "find the highest orders specific to the region" that you can use to practice your Spark queries:
# MAGIC
# MAGIC 1. **Identify the top 5 customers by total order value in each region.**
# MAGIC 2. **Calculate the average discount given on orders for each part category.**
# MAGIC 3. **Find the supplier who provides the most parts in terms of quantity.**
# MAGIC 4. **Determine the total revenue generated by each nation.**
# MAGIC 5. **List the top 10 parts with the highest sales volume.**
# MAGIC 6. **Find the average order value for each customer segment.**
# MAGIC 7. **Identify the regions with the highest number of orders placed.**
# MAGIC 8. **Calculate the total quantity of parts supplied by each supplier.**
# MAGIC 9. **Determine the most frequently ordered part in each region.**
# MAGIC 10. **Find the top 3 nations with the highest total order value.**
# MAGIC 11. **Calculate the total revenue generated by each supplier.**
# MAGIC 12. **Identify the parts with the highest average discount.**
# MAGIC 13. **Find the customers who placed the highest number of orders.**
# MAGIC 14. **Determine the total sales volume for each part category.**
# MAGIC 15. **Calculate the average order value for each region.**
# MAGIC
# MAGIC These problem statements should help you practice and enhance your Spark query skills. Let me know if you need further assistance with any specific problem!

# COMMAND ----------

# DBTITLE 1,Find the supplier who provides the most parts in terms of quantity.
supp_sale_most_part = max_view_order_table.groupby(
  "supplier_name"
).agg(
  f.sum("order_line_item_quantity").alias("total_sale_items")
).orderBy(f.desc("total_sale_items"))
display(supp_sale_most_part.take(5))

# COMMAND ----------

# DBTITLE 1,Determine the total revenue generated by each nation
total_revenue_by_region = max_view_order_table.groupby(
  "nation_name"
).agg(
  f.sum("order_billing_amount").alias("total_revenue")
).orderBy(f.desc("total_revenue"))
display(total_revenue_by_region.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ['customer_key',
# MAGIC  'nation_key',
# MAGIC  'region_key',
# MAGIC  'order_key',
# MAGIC  'part_key',
# MAGIC  'supplier_key',
# MAGIC  'customer_name',
# MAGIC  'customer_address',
# MAGIC  'customer_phone',
# MAGIC  'customer_acctbal',
# MAGIC  'mktsegment',
# MAGIC  'region_name',
# MAGIC  'nation_name',
# MAGIC  'order_status',
# MAGIC  'order_billing_amount',
# MAGIC  'order_date',
# MAGIC  'order_priority',
# MAGIC  'order_clerk',
# MAGIC  'order_ship_priority',
# MAGIC  'order_comment',
# MAGIC  'order_line_item_number',
# MAGIC  'order_line_item_quantity',
# MAGIC  'order_line_item_extended_price',
# MAGIC  'order_line_item_discount',
# MAGIC  'order_line_item_tax',
# MAGIC  'order_line_item_return_flag',
# MAGIC  'order_line_item_ship_date',
# MAGIC  'order_line_item_commit_date',
# MAGIC  'order_line_item_ship_mode',
# MAGIC  'part_name',
# MAGIC  'part_manufacturer',
# MAGIC  'part_brand',
# MAGIC  'part_type',
# MAGIC  'part_size',
# MAGIC  'part_container',
# MAGIC  'part_retail_price',
# MAGIC  'supplier_name',
# MAGIC  'supplier_address',
# MAGIC  'supplier_phone',
# MAGIC  'supplier_acctbal',
# MAGIC  'region_comment',
# MAGIC  'nation_comment',
# MAGIC  'customer_comment',
# MAGIC  'order_line_item_comment',
# MAGIC  'supplier_comment',
# MAGIC  'part_comment']

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import os

checkpoint_dir = "dbfs:/path/to/your/streaming/checkpoint"

# List contents of the main checkpoint directory
print("Main checkpoint directory contents:")
for f in dbutils.fs.ls(checkpoint_dir):
    print(f.path)

# List contents of the 'offsets' directory
offsets_dir = os.path.join(checkpoint_dir, "offsets")
print("\nOffsets directory contents:")
try:
    for f in dbutils.fs.ls(offsets_dir):
        print(f.path)
except Exception as e:
    print(f"Error listing offsets: {e}. Directory may not exist yet.")

# ... similarly for 'commits', 'metadata', 'state' ...
