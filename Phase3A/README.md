# 🧹 Phase 3A – Data Quality & Cleaning Challenge (PySpark)

## 📌 Overview
This task focuses on handling **real-world messy data** using PySpark.  
The objective is to identify data quality issues, apply cleaning techniques, and ensure the dataset is reliable for further processing and analysis.

This phase highlights the importance of **data quality in data engineering pipelines**.

---

## 🎯 Problem Understanding
In real-world scenarios, data is often incomplete, inconsistent, or incorrect.  
The given dataset contains multiple issues such as:

- Missing values (nulls)  
- Duplicate records  
- Invalid data (negative age)  

The goal is to clean this data before performing any analysis.

---

## 🛠️ Approach Taken
- Created a DataFrame with intentionally messy data  
- Analyzed the dataset to identify quality issues  
- Applied multiple data cleaning techniques  
- Validated the cleaned dataset  
- Performed aggregation to extract insights  

---

## 🔍 Data Issues Identified
- Null values in key columns (customer_id, name, city)  
- Duplicate records  
- Invalid values (negative age)  
- Incomplete records affecting analysis  

---

## 🔑 Cleaning Steps Performed

### 1. Handling Missing Values
- Removed rows where **customer_id** is null  
- Replaced missing values in **name** and **city** with default values  

### 2. Removing Duplicates
- Identified and removed duplicate records to ensure data uniqueness  

### 3. Filtering Invalid Data
- Removed records with invalid age values (age ≤ 0)  

---

## ✅ Validation
- Compared row counts before and after cleaning  
- Ensured only valid and meaningful records remain  
- Verified schema and data consistency  

---

## 📊 Final Analysis
- Performed aggregation to calculate **number of customers per city**  
- Ensured results are accurate after cleaning  

---

## 📘 Key Learnings
- Real-world data is often messy and requires preprocessing  
- Data cleaning is a critical step before analysis  
- Poor data quality leads to incorrect insights  
- Validation is essential after every transformation  

---

## ⚠️ Challenges Faced
- Identifying all possible data quality issues  
- Deciding how to handle missing values  
- Ensuring no important data is lost during cleaning  
- Validating correctness after transformations  

---

## 💡 Reflection

- **What happens if cleaning is skipped?**  
  Results become inaccurate and misleading  

- **Which issue impacted results most?**  
  Null values and invalid data significantly affect aggregations  

- **How would this affect business decisions?**  
  Poor data quality can lead to wrong decisions and financial loss  

- **Cleaning Checklist:**  
  - Remove null keys  
  - Handle missing values  
  - Remove duplicates  
  - Validate data types  
  - Filter invalid values  
  - Verify results after cleaning  

---

## 🚀 Conclusion
This task provided hands-on experience in **data cleaning and validation using PySpark**.  
It reinforces the importance of data quality in building reliable and scalable data pipelines.
