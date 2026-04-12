# 🚀 PySpark Practice Lab – Spark Playground Exit Sprint

## 📌 Overview
This project focuses on building strong PySpark fundamentals through hands-on practice. It covers key data engineering concepts such as joins, window functions, date transformations, and complete pipeline execution.

The objective is to simulate real-world data processing workflows and prepare for platforms like Databricks.

---

## ❓ Problem Understanding

### Customers Dataset
- Contains customer details such as:
  - customer_id
  - name
  - email
  - city

### Orders Dataset
- Contains transactional data such as:
  - order_id
  - customer_id
  - order_date
  - amount

### Objective
- Clean and validate data  
- Ensure referential integrity  
- Perform transformations and aggregations  
- Generate meaningful business insights  

---

## 🧠 Approach Taken

### 1. Data Loading
- Created DataFrames from structured data  
- Verified schema and row counts  

### 2. Data Cleaning
- Removed null values in critical fields  
- Filtered invalid records (negative and null amounts)  
- Standardized date formats  

### 3. Referential Integrity Validation
- Identified invalid foreign keys  
- Ensured all orders map to valid customers  

### 4. Joins
- Inner Join → valid records  
- Left Join → identify missing mappings  
- Left Anti Join → detect invalid references  

### 5. Transformations
- Aggregations (sum, count)  
- Window functions (rank, lag, running total)  
- Date functions (month extraction, date difference)  

### 6. Pipeline Execution
Clean → Validate → Join → Aggregate → Analyze → Save  

---

## ⚠️ Data Issues Identified
- Missing customer names  
- Missing emails  
- Null order amounts  
- Negative transaction values  
- Invalid foreign keys (customer_id not present in customers table)  
- Duplicate-like records  

---

## ✅ Validation
- Compared row counts across joins  
- Checked for null values after joins  
- Identified unmatched records  
- Verified aggregation outputs  

---

## 📊 Final Analysis
- Top customers per city  
- Total spend per customer  
- Monthly sales trends  
- Running totals of transactions  
- Previous order comparison using lag  

---

## 📚 Key Learnings
- Importance of data cleaning before joins  
- Understanding different join types  
- Effective use of window functions  
- Handling null values and edge cases  
- Building structured data pipelines  

---

## 🧩 Challenges Faced
- Choosing correct join types  
- Handling missing and inconsistent data  
- Debugging window function logic  
- Managing date conversions  
- Ensuring correctness at each step  

---

## 🔍 Reflection
- Window functions took the most time to understand  
- Debugging joins improved problem-solving skills  
- Validation steps prevented incorrect outputs  
- Gained confidence in building pipelines independently  

---

## 🏁 Conclusion
This project strengthened core PySpark skills and provided hands-on experience with real-world data scenarios.

It prepares for:
- Databricks workflows  
- Production-level pipelines  
- Large-scale data processing  
