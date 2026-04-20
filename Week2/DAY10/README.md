**End-to-End Incremental Data Pipeline using PySpark (Medallion Architecture Inspired)**

---

#### 🧭 Overview

This demonstrates how to build a **real-world data engineering pipeline** using **PySpark**, focusing on **incremental data loading**, **upsert logic**, and **data transformation using UDFs**.

The pipeline simulates daily incoming data and shows how to efficiently process **new and updated records** instead of reprocessing the entire dataset.

It loosely follows **Medallion Architecture principles** by handling raw ingestion and transformation into a refined dataset.

---

#### 🎯 Problem Understanding

In real-world systems, data arrives continuously. Processing full data every time leads to:

* High computation cost
* Slow performance
* Inefficient pipelines

Key challenges addressed:

* Identify **new vs updated records**
* Avoid **duplicate processing**
* Maintain **latest state of data**
* Apply **custom business logic (UDF)**

---

#### 🏗️ Approach Taken

##### Step 1: Full Load (Initial Setup)

* Load Day 1 dataset
* Store it as **Parquet**
* This acts as the **base dataset**

##### Step 2: Read Existing Data

* Load stored data from storage
* Extract **last_loaded_date dynamically**

##### Step 3: Incremental Load

* Load new incoming data (Day 2)
* Filter records where:

  ```
  updated_date > last_loaded_date
  ```
* This ensures only **new + updated records** are processed

##### Step 4: Handle Updates (UPSERT Logic)

* Remove old records using **left_anti join**
* Replace them with latest records

##### Step 5: Merge Data

* Combine:

  * Clean existing data
  * Incremental data
* Final dataset represents **latest state**

##### Step 6: Write Back

* Overwrite storage with updated dataset

##### Step 7: Apply Business Logic using UDF

* Created a custom function to assign grades
* Converted it into PySpark UDF
* Applied transformation on DataFrame

---

#### 🧠 Key Learnings

* Difference between **Full Load vs Incremental Load**
* Importance of **dynamic filtering (last_loaded_date)**
* How **left_anti join** helps in handling updates
* Concept of **UPSERT without Delta Lake**
* When and why to use **UDFs**
* Limitations of built-in functions and need for custom logic

---

#### 📊 Output Generated

Final dataset contains:

* Updated records (order_id 3, 4)
* Newly inserted records (6, 7, 8)
* Old unchanged records (1, 2, 5)

This ensures:

* No duplicates
* Latest data always available
* Efficient processing

---

#### ⚠️ Challenges Faced

* Handling updates without built-in MERGE (since using Parquet)
* Ensuring no duplicate records after union
* Understanding join types (especially left_anti)
* Managing dynamic date filtering
* Writing UDF with proper return types

---

#### 🚀 Improvements (Production Level)

* Replace Parquet with **Delta Lake** for native MERGE support
* Add **schema evolution** handling
* Implement **data quality checks**
* Use **partitioning** for performance optimization
* Schedule pipeline using **Databricks Jobs / Airflow**

---

#### 🧱 Medallion Architecture Mapping

* **Bronze Layer** → Raw ingestion (Day 1, Day 2 data)
* **Silver Layer** → Cleaned + deduplicated + upserted data (final_df)
* **Gold Layer (optional)** → Aggregations like revenue, top customers

---

#### 🧾 Conclusion

This project demonstrates how to design a **scalable and efficient incremental data pipeline** using PySpark.

It highlights:

* Real-world data challenges
* Practical implementation of incremental logic
* Foundation for building production-grade pipelines

---

#### 🔍 Reflection

Working on this pipeline strengthens understanding of:

* Data engineering workflows
* Optimization techniques
* Handling real-time data changes

It builds a strong base for transitioning into:

* **Delta Lake pipelines**
* **Streaming systems**
* **Production ETL architectures**
