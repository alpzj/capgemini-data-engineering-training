# 📊 Phase 2 – Data Transformation using SQL & PySpark

## 🎯 Problem Understanding
The objective of this task is to perform **data transformation and analysis** on customer and order datasets.  
The focus is on understanding how to derive meaningful insights using:

- SQL (structured queries)
- PySpark (distributed data processing)

This includes working with joins, aggregations, filtering, and handling missing data.

---

## 🛠️ Approach Taken
- Created and structured datasets for **customers** and **orders/sales**
- Performed data cleaning to handle null values
- Applied joins to combine datasets
- Used aggregation functions to calculate metrics
- Implemented the same logic in both SQL and PySpark
- Compared outputs to ensure correctness

---

## 🧩 Dataset Overview

### Customers Dataset
- Customer ID  
- Customer Name  
- City  

### Orders / Sales Dataset
- Order ID / Sale ID  
- Customer ID  
- Order Amount  

---

## 🔍 Operations Performed

- Calculated total order amount per customer  
- Identified top customers based on spending  
- Found customers with no orders  
- Computed city-wise revenue  
- Calculated average order value per customer  
- Identified customers with multiple orders  
- Sorted customers based on total spend  

---

## 🔑 Key Transformations

- **Data Cleaning**
  - Removed null values from key columns  
  - Converted data types for accurate calculations  

- **Joins**
  - Used left join to retain all customers  
  - Identified unmatched records (customers with no orders)  

- **Aggregations**
  - SUM → Total revenue  
  - AVG → Average order value  
  - COUNT → Number of orders  

- **Filtering**
  - Applied conditions to identify specific customer groups  

- **Sorting & Ranking**
  - Ordered customers based on total spending  

---

## 📘 Learnings

- Strong understanding of **joins and aggregations** in both SQL and PySpark  
- Learned how to handle **missing data and null values**  
- Understood how to translate SQL logic into PySpark transformations  
- Gained experience in **real-world data processing scenarios**  
- Improved knowledge of **data analysis using distributed systems**  

---

## ⚠️ Challenges Faced

- Translating SQL joins into PySpark DataFrame operations  
- Handling null values after joins  
- Understanding aggregation behavior in distributed processing  
- Managing data types (string to numeric conversion)  
- Debugging and validating results across SQL and PySpark  

---

## 🚀 Conclusion

This task helped in building a solid understanding of **data transformation techniques** using both SQL and PySpark.  
It also provided practical exposure to handling real-world data scenarios like joins, aggregations, and missing data.
