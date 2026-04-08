from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Phase3A_DataCleaning") \
    .getOrCreate()

# 2. Create DataFrame
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("Original Data")
df.show()

# 3. Identify Issues (basic check)
print("Schema")
df.printSchema()

print("Total Rows Before Cleaning:", df.count())

# 4. Cleaning

# Remove null customer_id
df_clean = df.dropna(subset=["customer_id"])

# Fill missing values
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# Remove duplicates
df_clean = df_clean.dropDuplicates()

# Remove invalid age
df_clean = df_clean.filter(col("age") > 0)

print("Cleaned Data")
df_clean.show()

# 5. Validation
print("Total Rows After Cleaning:", df_clean.count())

# 6. Aggregation
print("Customers per City")
df_clean.groupBy("city").count().show()

# 7. Stop Spark
spark.stop()
