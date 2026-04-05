from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
print('Setup complete')

display(dbutils.fs.ls('/Volumes/john/schema/vol'))

orders = spark.read.csv('/Volumes/john/schema/vol/olist_orders_dataset.csv', header=True, inferSchema=True)
order_items = spark.read.csv('/Volumes/john/schema/vol/olist_order_items_dataset.csv', header=True, inferSchema=True)
customers = spark.read.csv('/Volumes/john/schema/vol/olist_customers_dataset.csv', header=True, inferSchema=True)
products = spark.read.csv('/Volumes/john/schema/vol/olist_products_dataset.csv', header=True, inferSchema=True)
payments = spark.read.csv('/Volumes/john/schema/vol/olist_order_payments_dataset.csv', header=True, inferSchema=True)

display(orders)
display(customers)
display(order_items)
orders.printSchema()

orders.select([count(when(col(c).isNull(), c)).alias(c) for c in orders.columns]).show()
print('Orders:', orders.count())
print('Customers:', customers.count())

orders_customers = orders.join(customers, 'customer_id', 'inner')
display(orders_customers)

fact_orders = order_items \
    .join(orders, 'order_id') \
    .join(customers, 'customer_id') \
    .join(payments, 'order_id')

display(fact_orders)

#Queries

#=== TASK 1: Top 3 Customers per City === 

df = orders \
    .join(customers, "customer_id") \
    .join(order_items, "order_id")

customer_spend = df.groupBy("customer_id", "customer_city") \
    .agg(F.sum("price").alias("total_spend"))

windowSpec = Window.partitionBy("customer_city") \
    .orderBy(F.col("total_spend").desc())

ranked_df = customer_spend.withColumn(
    "rank", F.row_number().over(windowSpec)
)

top_3_customers = ranked_df.filter(F.col("rank") <= 3)

print("Top 3 customers in each city:")
top_3_customers.select(
    "customer_id", "customer_city", "total_spend"
).show()

#=== TASK 2: Running Total of Sales ===

df = orders.join(order_items, "order_id")

df = df.withColumn("date", F.to_date("order_purchase_timestamp"))

daily_sales = df.groupBy("date") \
    .agg(F.sum("price").alias("daily_sales"))

windowSpec = Window.orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = daily_sales.withColumn(
    "running_total",
    F.sum("daily_sales").over(windowSpec)
)

print("Daily sales with running total:")
result.select("date", "daily_sales", "running_total").show()

#=== TASK 3: Top Product per Category ===

df = order_items.join(products, "product_id")

df = df.withColumn("category", F.col("product_category_name"))

df = df.groupBy("category", "product_id") \
    .agg(F.sum("price").alias("total_sales"))

windowSpec = Window.partitionBy("category") \
    .orderBy(F.col("total_sales").desc())

ranked_df = df.withColumn(
    "rank", F.dense_rank().over(windowSpec)
)

top_products = ranked_df.filter(F.col("rank") == 1)

print("Top product in each category:")
top_products.select(
    "category", "product_id", "total_sales", "rank"
).show()

#=== TASK 4: Customer Lifetime Value ===

df = order_items.join(orders, "order_id")

customer_ltv = df.groupBy("customer_id") \
    .agg(F.sum("price").alias("total_spend"))

print("Customer Lifetime Value:")
customer_ltv.select("customer_id", "total_spend").show()

#=== TASK 5: Customer Segmentation ===

df = order_items.join(orders, "order_id")

customer_ltv = df.groupBy("customer_id") \
    .agg(F.sum("price").alias("total_spend"))

segmentation = customer_ltv.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when(F.col("total_spend") >= 5000, "Silver")
     .otherwise("Bronze")
)

print("Customer Segmentation (Detailed):")
segmentation.select("customer_id", "total_spend", "segment").show()

print("Customer count per segment:")
segment_count = segmentation.groupBy("segment") \
    .agg(F.count("customer_id").alias("customer_count"))

segment_count.show()
    
#=== TASK 6: Final Reporting Table ===

df = orders.join(order_items, "order_id") \
           .join(customers, "customer_id")

final_report = df.groupBy("customer_id", "customer_city") \
    .agg(
        F.sum("price").alias("total_spend"),
        F.countDistinct("order_id").alias("total_orders")
    )

final_report = final_report.withColumn(
    "segment",
    F.when(F.col("total_spend") > 10000, "Gold")
     .when(F.col("total_spend") >= 5000, "Silver")
     .otherwise("Bronze")
)

final_report = final_report.withColumnRenamed("customer_city", "city")

print("Final Business Reporting Table:")
final_report.select(
    "customer_id",
    "city",
    "total_spend",
    "segment",
    "total_orders"
).show()
