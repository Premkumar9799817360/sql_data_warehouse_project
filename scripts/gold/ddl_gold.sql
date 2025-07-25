
/*
===========================================================
 -- Dimension Table: 
===========================================================



 _______________________________________________

 -- 1.Customer
 ________________________________________________
*/

drop table if exists gold.dim_customer;
create view gold.dim_customer AS
SELECT 
 row_number() over(order by cst_id) AS customer_key,
  ci.cst_id AS customer_id,
  ci.cst_key AS customer_number,
  ci.cst_firstname AS first_name,
  ci.cst_lastname AS last_name,
  la.cntry as country,
  ci.cst_material_status as matital_status,
  case when ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        else coalesce(ca.gen,'n/a')
       end as gender,
  ca.bdate as birthday,
  ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_loc_a101 la
  ON ci.cst_key = la.cid
LEFT JOIN  silver.erp_cust_az12 ca
on ci.cst_key = ca.cid

/*_________________________________________________________

--2 Product Table
__________________________________________________________
 */
drop table if exists gold.dim_products;
create view gold.dim_products as 
select 
row_number() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.cat_id as category,
pn.prd_nm as category_id,
pc.cat as subcategory,
pc.subcat ,
pc.maintenance ,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date,
from silver.crm_prd_info pn 
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null  -- filter out all historical date

 /*
==================================================================
-- Fact Table
==================================================================
*/
 
drop table if exists gold.fact_sales
create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quanity,
sd.sls_price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id = cu.customer_id






