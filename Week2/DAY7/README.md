# 🚗 Car Sales Data Pipeline & Delta Lake Project

---

### 📌 Overview
This project demonstrates an **end-to-end data engineering pipeline using PySpark** along with **Delta Lake features**.

It focuses on:
- Handling messy real-world data
- Performing data cleaning and validation
- Building analytical datasets
- Applying Delta Lake operations like INSERT, UPDATE, DELETE, MERGE, and Time Travel

---

### ❗ Problem Understanding
Real-world datasets often contain:
- Invalid values (negative prices)
- Duplicate records
- Inconsistent formatting (extra spaces)
- Broken relationships (invalid foreign keys)

The goal was to:
- Clean and validate data
- Fix relationships between tables
- Build a reliable analytics pipeline

---

### ⚙️ Approach Taken

#### Phase 1 – Data Understanding
- Checked schema of all tables
- Counted records
- Performed null validation

#### Phase 2 – Data Cleaning
- Trimmed unwanted spaces in string columns
- Converted negative prices to positive values
- Removed duplicate records

#### Phase 3 – Data Validation
- Identified invalid foreign keys using joins
- Removed incorrect records using inner joins

#### Phase 4 – Transformation
- Built a **fact table**
- Calculated revenue using: revenue = price * quantity

#### Phase 5 – Analytics
- Customer revenue
- Brand-wise sales
- City-wise revenue
- Dealer performance analysis

#### Phase 6 – SQL Analysis
- Top 3 customers per city
- Monthly revenue trends
- Repeat customers

---

### 🔑 Key Learnings
- Importance of **data quality checks**
- Handling **joins and relationships correctly**
- Building **fact tables for analytics**
- Using **window functions for ranking**
- Writing both **PySpark and SQL queries**

---

### 📊 Outputs Generated

#### Customer Revenue
- Total revenue per customer

#### Brand Sales
- Revenue contribution by each car brand

#### City Revenue
- Revenue distribution across cities

#### Dealer Analytics
- Revenue per dealer
- Top 5 performing dealers

#### SQL Insights
- Top customers per city
- Monthly sales trends
- Repeat purchase behavior

---

### ⚠️ Challenges Faced
- Broken pipeline due to incorrect joins
- Handling invalid foreign keys
- Negative values affecting revenue
- Window function performance warning (no partition)

---

### 💡 Solutions Implemented
- Fixed join order in pipeline
- Used `left_anti` joins to detect invalid data
- Applied `abs()` to correct negative prices
- Used proper transformations before analytics

---

### 🚀 Delta Lake Implementation

#### Features Used:
- ✅ Table Creation
- ✅ INSERT (Append)
- ✅ UPDATE
- ✅ DELETE
- ✅ MERGE (Upsert)
- ✅ Schema Evolution
- ✅ Time Travel
- ✅ History Tracking

---

### 🔄 Delta Lake Workflow

#### 1. Create Table
- Stored data in Delta format

#### 2. Insert Data
- Appended new records

#### 3. Update Data
- Modified existing records

#### 4. Delete Data
- Removed unwanted records

#### 5. Merge Data
- Performed UPSERT (update + insert)

#### 6. Schema Evolution
- Added new column (`country`)

#### 7. Time Travel
- Accessed historical versions of data

---

### 📈 Final Output
- Cleaned and validated dataset
- Accurate revenue calculations
- Fully functional analytics pipeline
- Delta table with version history

---

### 🧠 Learnings
- Delta Lake enables **ACID transactions on big data**
- Time Travel helps in **debugging and auditing**
- MERGE simplifies **incremental data processing**
- Data validation is **critical before analytics**

---

### 🔍 Reflection
This project helped in understanding:
- Real-world data challenges
- End-to-end pipeline building
- Importance of clean data for analytics
- Practical use of Delta Lake in production scenarios

---

### 📌 Conclusion
This project successfully demonstrates:
- Building a **robust PySpark pipeline**
- Handling **data quality issues**
- Performing **advanced analytics**
- Leveraging **Delta Lake for reliable data engineering**
