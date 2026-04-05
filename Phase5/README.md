# 📊 Databricks End-to-End Data Engineering Pipeline (Olist Dataset)

## 🚀 Project Overview
This repository contains an end-to-end data engineering pipeline built using Databricks (PySpark) on the Olist Brazilian E-commerce Dataset.

The project demonstrates how raw multi-table data can be transformed into meaningful business insights using data engineering best practices such as ingestion, validation, transformation, and analytics.

This work is part of my training at Capgemini, where I am building hands-on experience in real-world data engineering workflows.

---

## 🎯 Objectives
- Work with a real-world multi-table dataset  
- Build scalable data pipelines using PySpark  
- Perform data cleaning and validation  
- Apply joins, aggregations, and window functions  
- Design analytical datasets for reporting  
- Generate business insights from raw data  

---

## 🛠️ Tech Stack
- Databricks Community Edition  
- PySpark (Spark SQL, DataFrame API)  
- Python  
- GitHub (Version Control)  

---

## ⚙️ Pipeline Architecture

### 1. Data Ingestion
- Uploaded all CSV files into Databricks FileStore (`/FileStore/olist/`)  
- Loaded data into Spark DataFrames  

### 2. Data Validation
- Checked schema consistency  
- Handled missing/null values  
- Ensured referential integrity between tables  

### 3. Data Transformation
- Joined multiple datasets (customers, orders, items, payments, products)  
- Created structured datasets for analysis  
- Applied aggregations and filtering  

### 4. Data Modeling
- Built fact and dimension-like structures  
- Fact: order-level transactional data  
- Dimensions: customer, product, seller  

---

## 📊 Analytical Tasks

### 🔹 Task 1: Top 3 Customers per City
- Calculated total spend per customer  
- Used window functions to rank customers within each city  

### 🔹 Task 2: Running Total of Sales
- Computed daily sales  
- Generated cumulative (running) total using window functions  

### 🔹 Task 3: Top Products per Category
- Aggregated product sales  
- Joined with product categories  
- Applied DENSE_RANK for ranking  

### 🔹 Task 4: Customer Lifetime Value (CLV)
- Calculated total spend across all orders per customer  

### 🔹 Task 5: Customer Segmentation
- Segmented customers based on spending:
  - Gold (> 10000)
  - Silver (5000–10000)
  - Bronze (< 5000)
- Counted customers per segment  

### 🔹 Task 6: Final Reporting Table
- Combined all key metrics into a single dataset:
  - customer_id  
  - city  
  - total_spend  
  - segment  
  - total_orders  

---

## 📈 Key Concepts Applied
- Window Functions (ROW_NUMBER, DENSE_RANK, SUM OVER)  
- Joins (Inner, Left)  
- Aggregations (GROUP BY, SUM, COUNT)  
- Data Modeling (Fact vs Dimension)  
- Referential Integrity  

---

## 📌 Key Outcomes
- Built a complete ETL pipeline in Databricks  
- Gained hands-on experience with PySpark transformations  
- Generated business-ready datasets  
- Improved understanding of scalable data processing
