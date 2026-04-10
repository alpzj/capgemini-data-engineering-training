# SQL Date & Time Analysis Project (Databricks Training)

## 📌 Overview
The goal of this module is to demonstrate how temporal data can be extracted, transformed, and analyzed to generate meaningful business insights such as delivery timelines, time-based filtering, and financial year classification.

---

## 🎯 Objectives
- Understand and work with SQL date and timestamp data types
- Extract meaningful components such as year, month, and day
- Perform time-based calculations and comparisons
- Implement business logic using date functions
- Analyze trends based on time dimensions

---

## 🗂️ Dataset Description (Conceptual)
The dataset represents an **orders system** containing:

- Order details (ID, customer name)
- Order date and timestamp
- Delivery date
- Order amount

This dataset simulates real-world transactional data where time plays a critical role in analytics.

---

## 🛠️ Key SQL Concepts Applied

### 1. Date Component Extraction
- Extracted **year, month, and day** from date columns
- Used both direct functions and standard SQL `EXTRACT()` method
- Derived readable formats like **month names** and **day names**

### 2. Day & Week Analysis
- Identified numeric representations of weekdays
- Compared different methods (`WEEKDAY` vs `DAYOFWEEK`)
- Classified days into business-relevant categories

### 3. Weekend Identification
- Filtered records falling on **Saturday and Sunday**
- Demonstrated multiple approaches for the same logic

### 4. Date Difference Calculations
- Calculated delivery duration using:
  - `DATEDIFF` for simple differences
  - `TIMESTAMPDIFF` for flexible unit-based calculations
- Measured operational efficiency in delivery timelines

### 5. Month-End Analysis
- Extracted the **last day of the month**
- Useful for financial reporting and monthly aggregations

### 6. Time-Based Filtering
- Filtered data by specific months (e.g., January)
- Enabled targeted time-series analysis

### 7. Financial Year Logic
- Implemented **custom financial year calculation**
- Applied conditional logic to map dates into fiscal periods
- Demonstrated real-world business logic implementation using `CASE`

---

## 📊 Key Insights & Outcomes
- Built strong understanding of handling **date and timestamp data**
- Learned multiple ways to extract and interpret time-based information
- Demonstrated how SQL can be used for **time-series analysis**
- Applied business logic such as financial year classification
- Improved ability to analyze operational metrics like delivery duration

---

## 🚀 Tools & Technologies
- SQL (Structured Query Language)
- Databricks Platform
- Built-in Date & Time Functions
- Conditional Logic (CASE statements)

---

## 📚 Learning Outcomes
Through this project, I developed the ability to:
- Work confidently with date and time data types
- Perform temporal analysis using SQL
- Apply real-world business logic to datasets
- Optimize queries for time-based insights
- Translate raw timestamps into meaningful analytics

---

## 🧠 Conclusion
This project highlights my ability to work with **time-based data in SQL**, which is a critical skill in data engineering and analytics. It demonstrates practical knowledge of **date manipulation, time calculations, and business logic implementation**, all of which are essential for building reliable data pipelines and reports.
