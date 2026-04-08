# ETL PIPELINE: CUSTOMERS + SALES ONLY

from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.window import Window

# 1. EXTRACT
spark = SparkSession.builder.appName("Customers-Sales ETL").getOrCreate()

customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/customers.csv")

sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/sales.csv")

# 2. INSPECT
print("Customers Data:")
customers.show(5)
customers.printSchema()

print("Sales Data:")
sales.show(5)
sales.printSchema()

# 3. TRANSFORM (CLEANING)

# Cast columns
sales = sales.withColumn("customer_id", functions.col("customer_id").cast("int")) \
             .withColumn("quantity", functions.col("quantity").cast("int")) \
             .withColumn("total_amount", functions.col("total_amount").cast("double"))

customers = customers.withColumn("customer_id", functions.col("customer_id").cast("int"))

# Remove nulls
sales_clean = sales.dropna(subset=["customer_id", "quantity", "total_amount"]) \
                   .filter((functions.col("quantity") > 0) & (functions.col("total_amount") > 0))

customers_clean = customers.dropna(subset=["customer_id"])

# 4. JOIN
joined = sales_clean.join(customers_clean, "customer_id")

# 5. BUSINESS LOGIC

# 5.1 Daily Sales
daily_sales = sales_clean.groupBy("sale_date") \
    .agg(functions.sum("total_amount").alias("daily_sales"))

# 5.2 City-wise Revenue
city_revenue = joined.groupBy("city") \
    .agg(functions.sum("total_amount").alias("revenue"))

# 5.3 Repeat Customers (>2 orders)
repeat_customers = sales_clean.groupBy("customer_id") \
    .agg(functions.count("*").alias("order_count")) \
    .filter(functions.col("order_count") > 2)

# 5.4 Highest Spending Customer per City
customer_spend = joined.groupBy("city", "customer_id") \
    .agg(functions.sum("total_amount").alias("total_spent"))

window_spec = Window.partitionBy("city").orderBy(functions.col("total_spent").desc())

top_customers = customer_spend.withColumn("rank", functions.row_number().over(window_spec)) \
    .filter(functions.col("rank") == 1)

# 5.5 Final Reporting Table
final_report = joined.groupBy("customer_id", "city") \
    .agg(
        functions.sum("total_amount").alias("total_spend"),
        functions.count("*").alias("order_count")
    )

# 6. OUTPUT

print("Daily Sales:")
daily_sales.show()

print("City Revenue:")
city_revenue.show()

print("Repeat Customers:")
repeat_customers.show()

print("Top Customers per City:")
top_customers.show()

print("Final Report:")
final_report.show()

# 7. LOAD
final_report.write.mode("overwrite").parquet("/tmp/final_report")

# 8. STOP
spark.stop()
