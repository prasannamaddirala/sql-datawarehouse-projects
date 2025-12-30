/* DDL Scripts :Create Gold Views
 Script Purpose:
  This script creates views for the gold layer ,in the data warehouse.
  The Gold Layer represents the final dimensions and fact tables(star schema) 
  Each view performs transformations and combines data from the silver layer 
  to produce clean,enchriched and business ready dataset .
  usage:
  These views can be quiried directly for analystics and reporting
  */
  --=====================================---
  --Create Dimension:gold.dim_customers---
  --=========================================--
IF  OBJECT_ID('gold.dim_customers','V') IS NOT NULL
     DROP VIEW gold.dim_customers;
GO
create view gold.dim_customers as 
SELECT 
    ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key, 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry  as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr!='na' THEN ci.cst_gndr --CRM IS THE MASTER FOR GENDER INFO
	     ELSE COALESCE(ca.gen,'n/a')
	END as gender,
	ca.bdate as birthdate ,
	ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
on ci.cst_key = la.cid;
IF  OBJECT_ID('gold.dim_products','V') IS NOT NULL
     DROP VIEW gold.dim_products;
GO
create view gold.dim_products as
select 
row_number() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name, 
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost ,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null;

IF  OBJECT_ID('gold.fact_sales','V') IS NOT NULL
     DROP VIEW gold.fact_sales;
GO
create view gold.fact_sales as 
SELECT
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
  
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
     sd.sls_price
 FROM  silver.crm_sales_details sd
 left join gold.dim_products pr 
 on sd.sls_prd_key = pr.product_number
 left join gold.dim_customers cu 
 on  sd.sls_cust_id=cu.customer_id;
