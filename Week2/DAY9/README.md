# 🏦 Insurance Data Pipeline using PySpark (Medallion Architecture)

## 📌 Overview
This project demonstrates an end-to-end **data engineering pipeline** built using **PySpark**, following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline processes raw insurance data, performs data cleaning and validation, and generates meaningful business insights such as **customer risk scores, city-level analytics, and agent performance**.

---

## 🎯 Problem Understanding
Real-world insurance data often contains:

- Missing values
- Negative or invalid financial values
- Broken relationships (foreign key issues)
- Duplicate or inconsistent records

These issues can lead to:
- Incorrect risk calculations
- Wrong business decisions
- Poor analytics quality

👉 Goal: Build a **robust pipeline** that ensures data is clean, reliable, and analytics-ready.

---

## 🏗️ Architecture Used (Medallion Architecture)

### 🥉 Bronze Layer (Raw Data)
- Loaded raw datasets:
  - Customers
  - Policies
  - Claims
  - Agents
  - Policy-Agent mapping
- No transformations applied

---

### 🥈 Silver Layer (Cleaned Data)
Performed data cleaning and standardization:

#### ✅ Data Cleaning
- Handled null values using `fillna()` and `dropna()`
- Removed duplicates using `dropDuplicates()`

#### ✅ Data Validation
- Identified negative values (premium, claim_amount)
- Replaced negative values with `0`
- Checked invalid ages

#### ✅ Data Standardization
- Trimmed strings
- Standardized text using `initcap()`
- Converted date columns to proper format

#### ✅ Integrity Checks
- Policies without customers
- Claims without policies
- Foreign key validation

---

### 🥇 Gold Layer (Business Insights)
Created aggregated datasets for analytics:

#### 📊 Customer Metrics
- Total Premium per customer
- Total Claim per customer
- Risk Score: Risk Score = Total Claim / Total Premium

#### 🌍 City-Level Insights
- Total premium per city
- Total claims per city
- City-wise performance comparison

#### 👨‍💼 Agent Performance
- Total premium handled by each agent
- Ranking agents using:
- ROW_NUMBER
- RANK
- DENSE_RANK

---

## ⚙️ Approach Taken

1. Loaded structured datasets into PySpark DataFrames  
2. Performed **schema validation and row count checks**  
3. Identified data quality issues:
 - Nulls
 - Negatives
 - Invalid relationships  
4. Cleaned and standardized data (Silver layer)  
5. Validated cleaned data using:
 - Anti joins
 - Record counts  
6. Built **analytical transformations (Gold layer)**  
7. Used **SQL + Window Functions** for advanced analytics  

---

## 🔍 Key Insights Generated

### 📈 Customer Insights
- High-risk customers identified using risk score
- Customers with increasing claim patterns detected

### 🌆 City Insights
- Chennai has highest claim volume
- Bangalore has highest premium collection

### 🧑‍💼 Agent Insights
- Top-performing agents ranked by premium handled
- Regional performance differences observed

---

## 📊 Outputs Generated

- `final_customer_df` → Customer-level metrics  
- `final_city_df` → City-level summary  
- `final_agent_df` → Agent performance  

---

## ⚠️ Challenges Faced

- Handling **negative financial values** correctly  
- Managing **invalid foreign key relationships**  
- Ensuring **no data loss during joins**  
- Maintaining consistency across multiple joins  
- Designing efficient aggregations using PySpark  

---

## 📚 Learnings

- Importance of **data quality in pipelines**
- Real-world usage of **Medallion Architecture**
- Hands-on experience with:
- PySpark DataFrames
- SQL transformations
- Window functions
- Understanding **data validation techniques**
- Designing scalable **data pipelines**

---

## 🚀 Conclusion

This project successfully demonstrates how to:

- Transform raw data into **trusted, analytics-ready datasets**
- Apply **industry-standard architecture (Medallion)**
- Generate meaningful **business insights**

It reflects real-world data engineering workflows used in modern data platforms like **Databricks**.

---

## 🔮 Future Improvements

- Implement incremental data loading using **MERGE**
- Add **Delta Lake features** (Time Travel, Versioning)
- Build dashboards using Power BI / Tableau
- Automate pipeline using **Databricks Jobs**

---

## 💡 Reflection

This project strengthened understanding of:

- Data cleaning at scale
- Data modeling and relationships
- Analytical thinking using PySpark

It bridges the gap between **theoretical learning and real-world implementation**, making it highly valuable for **Data Engineering and Data Analytics roles**.
