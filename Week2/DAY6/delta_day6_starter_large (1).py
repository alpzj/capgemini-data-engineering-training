
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# =========================================
# 🔹 Initial Dataset (60 records)
# =========================================

data = [(1, 'Eva', 2450), (2, 'Charlie', 3097), (3, 'Ivy', 1953), (4, 'Bob', 3924), (5, 'Bob', 4746), (6, 'Frank', 3120), (7, 'Frank', 1431), (8, 'Frank', 801), (9, 'Jack', 4388), (10, 'Eva', 3268), (11, 'Ivy', 2794), (12, 'Jack', 4153), (13, 'Alice', 4410), (14, 'Ivy', 2865), (15, 'Frank', 2337), (16, 'Grace', 2575), (17, 'Charlie', 2446), (18, 'Bob', 2462), (19, 'Frank', 682), (20, 'Charlie', 819), (21, 'Eva', 4654), (22, 'Jack', 3449), (23, 'Ivy', 4630), (24, 'Eva', 3654), (25, 'Bob', 2662), (26, 'Frank', 741), (27, 'Ivy', 3688), (28, 'Jack', 4729), (29, 'Alice', 2930), (30, 'Charlie', 1085), (31, 'David', 4096), (32, 'Helen', 523), (33, 'Jack', 868), (34, 'Eva', 3094), (35, 'Charlie', 4403), (36, 'Bob', 1953), (37, 'Frank', 1996), (38, 'Alice', 1930), (39, 'Grace', 4408), (40, 'Alice', 4145), (41, 'David', 2478), (42, 'Charlie', 3403), (43, 'David', 2553), (44, 'Grace', 2456), (45, 'Charlie', 3523), (46, 'Frank', 2434), (47, 'Charlie', 2844), (48, 'Jack', 3830), (49, 'Frank', 1889), (50, 'Grace', 3858), (51, 'Eva', 4345), (52, 'Alice', 1584), (53, 'Charlie', 1359), (54, 'Bob', 3538), (55, 'Charlie', 983), (56, 'Frank', 696), (57, 'Alice', 675), (58, 'Alice', 3153), (59, 'Alice', 1802), (60, 'Eva', 4376)]

df = spark.createDataFrame(data, ["id","name","amount"])

# Write as Delta table
df.write.format("delta").mode("overwrite").save("/tmp/delta_demo")

delta_df = spark.read.format("delta").load("/tmp/delta_demo")

print("Initial Data")
delta_df.show()

# =========================================
# 🔹 Incremental Dataset (for MERGE)
# =========================================

incremental_data = [
    (1, "Alice", 3000),   # update
    (2, "Bob", 4000),     # update
    (61, "NewCust1", 2000),  # insert
    (62, "NewCust2", 3500)   # insert
]

inc_df = spark.createDataFrame(incremental_data, ["id","name","amount"])

# =========================================
# 🔹 TASKS
# =========================================

# 1. INSERT new records

# 2. UPDATE existing records

# 3. DELETE records where amount < 1000

# 4. MERGE incremental dataset into main table
#    WHEN MATCHED THEN UPDATE
#    WHEN NOT MATCHED THEN INSERT

# 5. Add new column (category) in incremental dataset
#    and perform schema evolution

# 6. Use DESCRIBE HISTORY

# 7. Perform time travel and restore previous version

print("Start implementing step by step")
