# 🧹 PySpark Data Cleaning & Transformation Project (C1 Dataset)

## 📌 Overview
This project demonstrates **data cleaning, transformation, and standardization using PySpark** on a messy real-world dataset.

The dataset contains inconsistencies such as:
- Missing values represented as `"blank"` / `"Blank"`
- Inconsistent country names (`india`, `India`, `New York`)
- Incorrect date formats
- Duplicate records
- Unstructured data types (all columns as string)

The goal is to clean and transform the dataset into a **consistent and analysis-ready format**.

---

## 📂 Dataset Description
The dataset contains the following columns:

- `CustomerID` – Unique ID of customer  
- `Name` – Customer name  
- `Country` – Country/Region  
- `JoinDate` – Date of joining  
- `Sales` – Sales amount  
- `Category` – Product category  

---

## 🚀 Steps Performed

---

## 1️⃣ Load Dataset
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

df = spark.read.csv(
    "/Volumes/workspace/default/datasets/C1.csv",
    header=True
)

df.show()
```

---

## 2️⃣ Display Data
```python
df.display()
```

---

## 3️⃣ Check Schema
```python
df.printSchema()
```

📌 All columns are initially loaded as **string type**

---

## 4️⃣ Handle Missing Values
Replaced `"blank"` and `"Blank"` with NULL values:

```python
df.na.replace(["blank", "Blank"], None).display()
```

---

## 5️⃣ Standardize Country Names
Converted inconsistent country values into proper format:

```python
from pyspark.sql.functions import col, when, upper, initcap

df = df.withColumn(
    "Country",
    when(col("Country") == upper(col("Country")), col("Country"))
    .otherwise(initcap(col("Country")))
)

df.display()
```

---

## 6️⃣ Convert Date Format
Converted `JoinDate` into proper date format:

```python
from pyspark.sql.functions import coalesce, to_date

df.withColumn(
    "JoinDate",
    coalesce(
        to_date(col("JoinDate"), "dd-MM-yyyy"),
        to_date(col("JoinDate"), "MM-dd-yyyy")
    )
).display()
```

---

## 7️⃣ Replace Incorrect Country Value
Replaced `"New York"` with `"USA"`:

```python
df.replace("New York", "USA").display()
```

---

## 8️⃣ Remove Duplicates
```python
df.dropDuplicates().display()
```

---

## 9️⃣ Filter Records
Removed a specific record:

```python
df = df.filter(col("CustomerID") != 104)
```

---

## 🔟 Sort Data
```python
df = df.orderBy("CustomerID")
df.display()
```

---

## 📊 Final Output
After cleaning, the dataset becomes:
- Consistent
- Structured
- Duplicate-free
- Standardized

### Sample Output:

| CustomerID | Name    | Country | JoinDate   | Sales | Category |
|------------|--------|--------|------------|-------|----------|
| 101        | Alice  | USA    | 15-01-2023 | 250   | A        |
| 102        | Bob    | India  | 01-05-2023 | 150   | B        |
| 103        | Charlie| UK     | 20-02-2023 | blank | C        |
| 105        | Eve    | UK     | 01-03-2023 | 300   | Blank    |

---

## 🧠 Key Learnings
- Real-world data is messy and inconsistent  
- PySpark provides powerful tools for large-scale data cleaning  
- Standardization is critical for analysis-ready data  
- Handling missing values improves data reliability  
- Transformations must be validated step-by-step  

---

## ⚠️ Challenges Faced
- Handling inconsistent text formats  
- Dealing with multiple date formats  
- Missing value representation as strings  
- Ensuring transformations do not break data integrity  

---

## 🚀 Conclusion
This project demonstrates how to clean and transform real-world messy data using **PySpark DataFrame operations**.

It highlights the importance of:
- Data standardization  
- Missing value handling  
- Data validation  
- Proper transformation pipelines  

---
