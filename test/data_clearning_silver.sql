/*

===============================================================================
For Snowflake 
===============================================================================
  */

-- For CRM Cust_info
-- Check for nulls or duplicates in primary key
-- Expectation: No result
select cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) >1 or cst_id is null;

--check for unwanted Spaces
-- Expectation: No Results
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr)

-- Data Standardization & Consistency
select distinct cst_gndr
from bronze.crm_cust_info

-- For CRM prd_info

-- Check for nulls or duplicates in primary key
-- Expectation: No result
select prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null

--check for unwanted spacese 
-- Expectation: No result
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)


-- data standardization & Consistency
SELECT distinct prd_line
FROM bronze.crm_prd_info

-- check for invalid date orders
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

-- For CRM sales details

select 
nullif(sls_order_dt,0) as sls_order_dt 
from bronze.crm_sales_details
where sls_order_dt <= 0
or length(sls_order_dt) != 8 -- 20200123 (2020-01-23)
or sls_order_dt > 20500101  -- (2050-01-01)
or sls_order_dt < 19000101  --(1900-01-01)

-- ABOVE QUERY USE FOR SAME SALSE SHIP AND SALES DUE DATE

-- Check for Invalid date Orders
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- check data consistency: Between sales, Quantity, and Price
--  sales = Quantity * Price
-- values must not be null, zero, or negative.

select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
    then  sls_quantity * abs(sls_price)
    else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <=0 
     then sls_sales / nullif(sls_quantity,0)
else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity*sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity is null or sls_price is null


-- For ERP erp_cust_az12
-- identify out -of range dates
select distinct
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

-- data standardization & consistency
select distinct gen,
case when upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
    when upper(trim(gen)) IN ('M','MALE') THEN 'Male'
    else 'n/a'
    end as new_gen
from bronze.erp_cust_az12


-- for ERP second table erp_loc_a101
select 
replace (cid,'-','') cid,
case when trim(cntry) = 'DE' then 'Germany'
    when trim(cntry) in ('US','USA') then 'United States'
    when trim(cntry) = '' or cntry is null then 'n/a'
    else trim(cntry)
    end as cntry
from bronze.erp_loc_a101

--  data standardization & consistency
select distinct cntry
from bronze.erp_loc_a101
order by cntry

--detele because columns add as first row 
delete from bronze.erp_loc_a101 where CID = 'CID' 



/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;














































