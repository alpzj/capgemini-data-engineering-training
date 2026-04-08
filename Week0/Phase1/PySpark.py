from pyspark.sql import SparkSession

# Create a Spark session (entry point to PySpark)
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Create a DataFrame with sample customer data
customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28)
], ["customer_id", "customer_name", "city", "age"])

# Display the entire DataFrame
customers.show()

# Filter rows where city is 'Chennai'
customers.filter(customers.city == "Chennai").show()

# Filter rows where age is greater than 25
customers.filter(customers.age > 25).show()

# Select only specific columns: customer_name and city
customers.select("customer_name", "city").show()

# Group data by city and count number of customers in each city
customers.groupBy("city").count().show()
