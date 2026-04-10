# 📊 SQL & PySpark Data Engineering Practice Project

## 📌 Overview
This project demonstrates hands-on practice in **SQL, PySpark DataFrames, and data analysis concepts** as part of data engineering training.

It covers:
- SQL table creation and joins
- Aggregations and group by operations
- Window-style analytical thinking
- PySpark DataFrame transformations
- Real-world employee–sales dataset modeling

The goal is to strengthen understanding of **data manipulation, joins, aggregations, and analytics using SQL + PySpark**.

---

## 🏗️ Dataset Overview

This project uses three main datasets:

### 1️⃣ Employee Table
Contains employee details:
- emp_id
- emp_name
- department
- salary
- joining_date

### 2️⃣ Sales Table
Contains employee sales transactions:
- sales_id
- emp_id
- product
- amount
- sale_date

### 3️⃣ Organizational Tables
- employees (hierarchical structure with manager_id)
- departments
- projects

---

## ⚙️ Technologies Used
- SQL (DDL & DML operations)
- PySpark (DataFrame API)
- Databricks / Spark Environment
- Aggregation & Join Operations

---

## 🧱 Step 1: Table Creation (SQL)

Created structured tables for Employee and Sales data using SQL:

- Employee table with primary key constraint
- Sales table with foreign key reference (emp_id)

Data inserted for analytical operations.

---

## 📊 Step 2: Data Exploration
- Displayed Employee and Sales tables
- Verified schema and structure
- Ensured data consistency before analysis

---

## 📈 Step 3: Aggregation Queries (SQL & PySpark)

Performed multiple business-level aggregations:

### ✔ Department-wise Analysis
- Total salary per department
- Average salary per department
- Maximum & minimum salary per department
- Employee count per department

### ✔ Filtering with Conditions
- Departments with salary > 100,000
- Departments with more than 2 employees
- Departments with high average salary (> 60,000)
- Salary range filtering using HAVING clause

---

## 🔗 Step 4: Join Operations

### Employee–Sales Analysis
- Total sales per employee
- Number of sales per employee
- Top performing employees
- Employees with no sales

### Product-Level Insights
- Total sales per product
- Average sales per product
- Products with high revenue (> 50,000)

### Department-Level Sales Analysis
- Total sales per department
- Highest selling department
- Average sales by department

---

## 🧠 Step 5: Advanced SQL Scenarios

Implemented real-world analytical queries:

- Employees without managers
- Hierarchical employee–manager mapping
- Employees without departments
- Employees without projects
- Full outer joins for complete data coverage
- Department-wise employee distribution
- Multi-table joins (Employee + Department + Projects)

---

## 🐍 Step 6: PySpark Implementation

Converted SQL logic into PySpark DataFrame operations:

### Transformations Performed:
- groupBy() aggregations
- filter() conditions
- join() operations
- withColumn() transformations
- type conversion (to_date)

### Example Operations:
- Total salary per department
- Average salary per department
- Employee count filtering
- Sales aggregation per employee
- Department-wise analytics

---

## 📌 Key SQL Concepts Covered

- SELECT, WHERE, GROUP BY, HAVING
- INNER JOIN, LEFT JOIN, FULL OUTER JOIN
- Aggregate functions (SUM, AVG, MIN, MAX, COUNT)
- Subquery-style filtering
- NULL handling using COALESCE
- Data filtering and sorting

---

## 📌 Key PySpark Concepts Covered

- SparkSession creation
- DataFrame creation from Python lists
- Schema definition using StructType
- Column transformations
- Aggregation using groupBy().agg()
- Filtering and conditional logic
- Date type conversion

---

## 📊 Business Insights Derived

- HR department has highest total salary distribution
- Engineering shows high salary variance
- Certain employees have no sales records
- Some employees are not assigned projects or departments
- Sales performance varies significantly by product type
- Department-level contribution to revenue can be identified

---

## ⚠️ Challenges Faced

- Handling NULL relationships in joins
- Ensuring correct aggregation logic in grouped queries
- Managing missing relationships across tables
- Converting SQL logic into PySpark syntax
- Debugging schema mismatches in Spark SQL

---

## 🚀 Conclusion

This project demonstrates strong foundational skills in:
- SQL querying and analytics
- PySpark DataFrame processing
- Real-world relational data modeling
- Business intelligence reporting logic
