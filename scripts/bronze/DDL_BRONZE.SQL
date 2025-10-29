-- fOR SNOWFLAKE ONLINE 
-- step 1
create database DataWarehouse;
use  DataWarehouse;
create schema bronze;
create schema silver
create schema gold;
SHOW SCHEMAS;

-- step 2  create all table (DDL)

-- first folder CRM data

-- TABLE 1 FOR CRM SOURCE
DROP TABLE IF EXISTS bronze.crm_cust_info;
create table bronze.crm_cust_info
(
cst_id INT,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_material_status varchar(50),
cst_gndr varchar(50),
cst_create_date DATE
);


-- TABLE 2 FOR CRM SOURCE
DROP TABLE IF EXISTS bronze.crm_prd_info;
create table bronze.crm_prd_info
(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt DATE,
prd_end_dt DATE
);


-- TABLE 3 FOR CRM SOURCE
DROP TABLE IF EXISTS bronze.crm_sales_details;
create table bronze.crm_sales_details
(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id  int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);


-- first folder ERM data

-- TABLE 1 FOR ERM SOURCE
DROP TABLE IF EXISTS bronze.erp_loc_a101;
second folder ERP Data
create table bronze.erp_loc_a101(
cid varchar(50),
cntry varchar(50)
);

-- TABLE 2 FOR ERM SOURCE
DROP TABLE IF EXISTS bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid varchar(50),
bdate DATE,
gen varchar(50)
)

-- TABLE 3 FOR ERM SOURCE
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id varchar(50),
cat varchar(50),
subcat  varchar(50),
maintenance varchar(50)
);


-- FOR MY SQL SERVER USE BELOW LINE OF CODE

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO









