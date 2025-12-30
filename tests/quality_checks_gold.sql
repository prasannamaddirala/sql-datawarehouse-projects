
/*Quality Checks 
Script Purpose :
This script performs quality checks to validate the integrity ,consistency ,
and accuracy of the gold layer .These checks ensure:
--uniquesness of surrogate keys in dimensions tables
--referential integrity between fact and dimensional tables
--validation of relationships in the data model for analystical purposes
Usage Notes:
  -Run these checks after data loading silver layer
  --Investigae and resolve desperencies found during the checks
  */
  --==============================================----
  --checking gold.dim.customers--
  --=====================================---
  --check for uniquness of customer key in gold.dim_customers---

  
select cst_id,count(*)
from(
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry 
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
on ci.cst_key = la.cid) t 
group by cst_id 
having count(*)>1;

/* checking the gender of both tables*/
SELECT distinct
	ci.cst_gndr,
	ca.gen

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
on ci.cst_key = la.cid ORDER BY 1,2; 

/* SOLUTION FOR THE ABOVE*/
SELECT distinct
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr!='na' THEN ci.cst_gndr --CRM IS THE MASTER FOR GENDER INFO
	     ELSE COALESCE(ca.gen,'n/a')
	END as new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
on ci.cst_key = la.cid ORDER BY 1,2; 

select distinct gender from gold.dim_customers;
  --==============================================----
  --checking gold.dim_products--
  --=====================================---
  --check for uniquness of product key in gold.dim_products---
  /* unique product key*/
select prd_key,count(*) from(
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null ---Filter out all historical dat
)t  group by prd_key 
having count(*)>1;

/* unique product key*/
select prd_key,count(*) from(
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null ---Filter out all historical dat
)t  group by prd_key 
having count(*)>1;

  --==============================================----
  --checking gold.fact_sales--
  --=====================================---
  --check for uniquness of product key in gold.fact_sales---
  /* foreign key integrity (dimensions)*/
select *
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key 
left join gold.dim_products p 
on p.product_key=f.product_key
where p.product_key is null;
