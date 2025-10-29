# Data Warehouse and Analytics Project 🚀

![Project Banner](https://via.placeholder.com/1200x300/4A90E2/FFFFFF?text=Data+Warehouse+%26+Analytics+Project)

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

#### **CRM Source**
- `cust_info.csv` - Customer information
- `prd_info.csv` - Product information
- `sales_details.csv` - Sales transaction details

#### **ERP Source**
- `CUST_AZ12.csv` - Customer demographics (Customer ID, Birthday, Gender)
- `LOC_A101.csv` - Customer location data (Customer ID, Country)
- `PX_CAT_G1V2.csv` - Product categories, subcategories, and maintenance flags

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

![Architecture Diagram](https://via.placeholder.com/800x500/2ECC71/FFFFFF?text=Medallion+Architecture:+Bronze+→+Silver+→+Gold)

### **Medallion Architecture Layers**

#### **1. Bronze Layer (Raw Data)**
- **Purpose**: Store raw data as-is from source systems
- **Process**: 
  - Created database and schemas (Bronze, Silver, Gold)
  - Created tables for all CRM and ERP source files using DDL commands
  - Loaded data using stored procedure with `COPY INTO` command
  - Implemented batch tracking with start/end timestamps

**Stored Procedure**: `bronze.load_bronze`

```sql
-- Usage Example
CALL bronze.load_bronze();
```

**Key Actions**:
- Truncates bronze tables before loading
- Bulk inserts data from CSV files into bronze tables
- Tracks batch execution time

---

#### **2. Silver Layer (Cleansed Data)**
- **Purpose**: Transform and cleanse data from Bronze layer
- **Process**:
  - Created tables in Silver schema using DDL commands
  - Applied data transformations and cleansing rules
  - Loaded data using `INSERT INTO` statements

**Stored Procedure**: `silver.load_silver`

```sql
-- Usage Example
CALL silver.load_silver();
```

**Key Actions**:
- Truncates Silver tables
- Inserts transformed and cleansed data from Bronze tables
- Standardizes data formats and applies business rules

---

#### **3. Gold Layer (Analytics-Ready Data)**
- **Purpose**: Create business-ready dimension and fact tables using Star Schema
- **Process**:
  - Created views for dimension and fact tables
  - Joined and enriched data from Silver layer

**Star Schema Design**:

**Dimension Tables**:
1. `gold.dim_customer` - Customer dimension
   - Sources: `silver.crm_cust_info`, `silver.erp_cust_az12`, `silver.erp_loc_a101`
   - Join Type: LEFT JOIN

2. `gold.dim_products` - Product dimension
   - Sources: `silver.crm_prd_info`, `silver.erp_px_cat`

**Fact Table**:
- `gold.fact_sales` - Sales transactions fact table
  - Source: `silver.crm_sales_details`
  - Joins: LEFT JOIN with `gold.dim_products` and `gold.dim_customer`

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

## 🚀 Getting Started

### Prerequisites
- Snowflake account
- Access to source CSV files
- Basic knowledge of SQL and data warehousing concepts

### Setup Steps

1. **Create Database and Schemas**
   ```sql
   CREATE DATABASE dw_project;
   CREATE SCHEMA bronze;
   CREATE SCHEMA silver;
   CREATE SCHEMA gold;
   ```

2. **Load Bronze Layer**
   - Create bronze tables using DDL scripts
   - Execute `bronze.load_bronze` stored procedure

3. **Load Silver Layer**
   - Create silver tables using DDL scripts
   - Execute `silver.load_silver` stored procedure

4. **Create Gold Layer**
   - Execute DDL scripts to create Gold views
   - Views are ready for querying and analytics

---

## 📈 Usage

Query the Gold layer for analytics and reporting:

```sql
-- Example: Get total sales by customer
SELECT 
    c.customer_name,
    SUM(f.sales_amount) as total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_name
ORDER BY total_sales DESC;
```

---

## 🤝 Contributing

Feel free to fork this repository and submit pull requests for improvements!

---

## 📧 Contact

For questions or feedback, please reach out through GitHub issues.

---

![Footer](https://via.placeholder.com/1200x100/34495E/FFFFFF?text=Thank+You+for+Visiting!)

**⭐ If you find this project helpful, please give it a star!**
