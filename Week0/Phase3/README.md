# 🚀 Phase 3 – ETL Pipeline (SQL → PySpark)

## 📌 Overview
This phase focuses on building a complete **ETL (Extract, Transform, Load) pipeline** using SQL and PySpark.

The objective is to move from writing individual queries to designing a **scalable data pipeline** that processes raw data into meaningful business insights.

---

## 🎯 Problem Understanding
Real-world data engineering involves:
- Reading raw data from multiple sources  
- Cleaning and validating the data  
- Applying business logic  
- Generating structured outputs for reporting  

This phase simulates a real-world pipeline using **customers and sales datasets**.

---

## 🏗️ ETL Architecture

```
        ┌──────────────┐
        │  Data Source │
        │ (CSV Files)  │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │   Extract    │
        │ Read CSV     │
        │ (Spark)      │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │  Transform   │
        │ - Clean Data │
        │ - Remove Nulls
        │ - Filter     │
        │ - Join Tables│
        │ - Aggregation│
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │   Business   │
        │   Logic      │
        │ - Daily Sales│
        │ - City Revenue
        │ - Repeat Cust│
        │ - Top Cust   │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │     Load     │
        │ Save as      │
        │ Parquet      │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │   Reporting  │
        │ Final Tables │
        └──────────────┘
```

---

## 🛠️ Approach Taken
- Ingested data from CSV files  
- Inspected schema and identified issues  
- Cleaned data by removing nulls and invalid values  
- Joined datasets to combine customer and sales data  
- Applied business transformations  
- Generated final reporting dataset  
- Stored output in optimized format (Parquet)  

---

## 🔑 Key Transformations

### Data Cleaning
- Removed null values from key columns  
- Filtered invalid records (negative or zero values)  
- Ensured correct data types  

### Data Integration
- Joined customers and sales datasets  
- Ensured referential consistency  

### Aggregations & Business Logic
- Daily sales calculation  
- City-wise revenue  
- Repeat customers identification  
- Highest spending customer per city  
- Final reporting table (customer-level metrics)  

---

## 📊 Outputs Generated
- Daily sales summary  
- Revenue per city  
- Repeat customers list  
- Top customers per city  
- Final reporting dataset  

---

## 📘 Learnings
- Understood complete **ETL pipeline lifecycle**  
- Learned how to design **scalable data workflows**  
- Gained experience in **data cleaning and validation**  
- Improved skills in **joins, aggregations, and window functions**  
- Learned how to move from SQL logic to PySpark pipelines  

---

## ⚠️ Challenges Faced
- Designing end-to-end pipeline flow  
- Handling null and inconsistent data  
- Implementing window functions in PySpark  
- Managing joins and aggregations efficiently  
- Ensuring output accuracy after transformations  

---

## 🚀 Conclusion
This phase helped in building a strong foundation in **data pipeline development** and understanding how real-world ETL systems are designed and implemented.
