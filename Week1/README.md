# SQL Data Engineering Practice – Databricks Training

## 📌 Overview
This repository showcases my hands-on SQL practice as part of my **Capgemini Databricks Training**. It covers core data engineering concepts including **data cleaning, transformation, NULL handling, date-time analysis, and pattern extraction using regular expressions**.

The project reflects real-world scenarios where raw, inconsistent, and partially missing data is transformed into structured, analysis-ready datasets.

---

## 🎯 Objectives
- Handle missing and inconsistent data effectively
- Perform data transformation and standardization
- Apply business logic using SQL
- Work with date and time functions for temporal analysis
- Extract meaningful patterns from semi-structured text data
- Build strong foundational skills for data engineering workflows

---

## 🗂️ Datasets (Conceptual Overview)

### 1. Employee Dataset
- Contains employee details such as salary, bonus, and manager hierarchy
- Includes multiple NULL scenarios for real-world simulation

### 2. Orders Dataset
- Customer transactions with order amount, discount, and coupon data
- Used for financial calculations and NULL handling

### 3. Products Dataset
- Product catalog with price, category, and stock details
- Includes incomplete and inconsistent records

### 4. Orders Date Dataset
- Focused on date and timestamp analysis
- Includes order date, delivery date, and timestamps

### 5. Regex Practice Dataset
- Designed for pattern extraction and string parsing
- Contains mixed text, emails, phone numbers, and coded identifiers

---

## 🛠️ Key SQL Concepts Applied

### 🔹 1. NULL Handling & Data Cleaning
- Identifying missing values using conditional filters
- Replacing NULL values using `COALESCE`
- Using `NULLIF` for conditional null conversion
- Handling edge cases in calculations

### 🔹 2. Data Transformation
- Creating derived columns for business insights
- Standardizing inconsistent data formats
- Combining multiple fields for meaningful outputs

### 🔹 3. Business Logic Implementation
- Calculating employee earnings (salary + bonus)
- Computing final payable amounts after discounts
- Handling fallback logic using priority-based values

### 🔹 4. Date & Time Analysis
- Extracting year, month, and day from dates
- Using `EXTRACT()` for standard SQL compatibility
- Identifying weekdays and weekends
- Calculating delivery durations using `DATEDIFF` and `TIMESTAMPDIFF`
- Performing month-based filtering
- Implementing financial year logic using `CASE`
- Deriving month-end dates

### 🔹 5. Regular Expressions (Regex)
- Extracting numeric and alphabetic patterns
- Parsing structured identifiers
- Extracting email usernames and domains
- Identifying country codes from phone numbers
- Capturing prefixes, suffixes, and embedded values

### 🔹 6. String Processing
- Extracting first and last characters
- Splitting structured text fields
- Identifying continuous sequences of letters and numbers

---

## 📊 Key Outcomes
- Successfully handled complex NULL scenarios across multiple datasets
- Built strong understanding of data cleaning techniques
- Applied real-world business logic using SQL
- Performed detailed time-based analysis
- Extracted structured insights from unstructured text
- Strengthened problem-solving skills in SQL

---

## 🚀 Tools & Technologies
- SQL (Structured Query Language)
- Databricks Platform
- Built-in SQL Functions
- Regular Expressions (REGEXP)
- Data Transformation Techniques

---

## 📚 Learning Highlights
- Writing efficient and optimized SQL queries
- Handling real-world messy datasets
- Applying analytical thinking to data problems
- Understanding how SQL fits into data engineering pipelines
- Working with both structured and semi-structured data

---

## 🧠 Conclusion
This repository demonstrates my practical knowledge of **SQL for data engineering**, including data cleaning, transformation, temporal analysis, and pattern extraction. It reflects my ability to convert raw data into meaningful insights, a key skill for any data professional.
