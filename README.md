# Data Warehouse and Analytics Project 🚀


Welcome to the Data Warehouse and Analytics Project! This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights.

---

## 📋 Project Overview

This project showcases:

- **Data Architecture**: Modern Data Warehouse using Medallion Architecture (Bronze, Silver, and Gold layers)
- **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse
- **Data Modeling**: Developing fact and dimension tables optimized for analytical queries using Star Schema
- **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights

---

## 🎯 Skills Demonstrated

This repository is perfect for professionals and students looking to showcase expertise in:

- SQL Development
- Data Architecture
- Data Engineering
- ETL Pipeline Development
- Data Modeling
- Data Analytics

---

## 🗂️ Dataset Structure

### Source Data Folders

### 🧾 CRM Source Files
| File Name | Description |
|------------|--------------|
| `cust_info.csv` | Customer information details |
| `prd_info.csv` | Product information data |
| `sales_details.csv` | Sales transaction details |

### 🧾 ERP Source Files
| File Name | Description |
|------------|--------------|
| `CUST_AZ12.csv` | Customer details (ID, birthday, gender) |
| `LOC_A101.csv` | Customer location (CID, country) |
| `PX_CAT_G1V2.csv` | Product category, sub-category, maintenance flag (Yes/No) |


---

## 🛠️ Technology Stack

### **Snowflake Data Platform**

This project uses **Snowflake**, a cloud-based data warehousing platform that provides:
- Scalable compute and storage
- Built-in support for structured and semi-structured data
- Easy data sharing and collaboration
- High performance for analytical queries

---

## 🏗️ Project Architecture

![Architecture Diagram](https://github.com/Premkumar9799817360/sql_data_warehouse_project/blob/main/data_architecture_image.png)

### **Medallion Architecture Layers**

### 1️⃣ Bronze Layer (Raw Data Layer)
**Purpose:** Store raw data from source systems.

**Tasks:**
- Create database and schemas (`bronze`, `silver`, `gold`).  
- Load CSV files from CRM and ERP sources into **Bronze tables**.  

#### 📜 Stored Procedure: Load Bronze Layer (Source → Bronze)
**Script Purpose:**
- Loads raw data from external CSVs into `bronze` schema tables.  
- Truncates bronze tables before reloading.  
- Performs **bulk loading**:
  - **Snowflake:** Uses `COPY INTO` command.
  - **SQL Server:** Uses `BULK INSERT` command.

**Tracking:** Includes start and end timestamps to record batch load duration.

**Usage:**
```sql
-- Snowflake
CALL bronze.load_bronze();

-- SQL Server
EXEC bronze.load_bronze;
```
DDL Example:
Create tables for all datasets (CRM + ERP files) using standard DDL commands.
---

## 2️⃣ Silver Layer (Cleaned & Transformed Data)

**Purpose:** Store cleansed, validated, and standardized data.

### 📜 Stored Procedure: Load Silver Layer (Bronze → Silver)

**Script Purpose:**
- Performs ETL transformation from Bronze to Silver.  
- Truncates Silver tables before insert.  
- Cleanses and joins related datasets.  

**Implementation:**
- Uses `INSERT INTO ... SELECT ...` logic for transformation.  
- Prepares refined data for analytics.  

**DDL Example:**  
Create tables for all Silver-layer entities using SQL DDL scripts.


---

## 3️⃣ Gold Layer (Analytics-Ready Layer)

**Purpose:** Provide business-ready dimension and fact tables for reporting.

### 📜 DDL Script: Create Gold Views (Silver → Gold)

**Script Purpose:**
- Creates views for analytical use.  
- Combines and enriches data from the Silver layer.  
- Follows **Star Schema** structure with **Fact** and **Dimension** tables.  

### 🟡 Star Schema Design

| Layer | Table | Description |
|--------|--------|-------------|
| Gold | `dim_customer` | Combines customer details from `silver.crm_cust_info`, `silver.erp_cust_az12`, and `silver.erp_loc_a101` |
| Gold | `dim_products` | Merges product details from `silver.crm_prd_info` and `silver.erp_px_cat` |
| Gold | `fact_sales` | Links sales with customer and product dimensions using foreign keys |


---

## 📊 Data Flow

```
Source Systems (CRM/ERP CSV Files)
           ↓
    Bronze Layer (Raw Data)
           ↓
    Silver Layer (Cleansed Data)
           ↓
    Gold Layer (Star Schema - Analytics Ready)
           ↓
    Reports & Dashboards
```

---
## ⭐ Star Schema

![Footer](https://github.com/Premkumar9799817360/sql_data_warehouse_project/blob/main/star%20schma.png)

**⭐ If you find this project helpful, please give it a star!**
