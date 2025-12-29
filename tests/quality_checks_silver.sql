
/*
***********************************************************
Quality Checks 
************************************************************
Script Purpose:
        This script purpose various quality checks for data consistency ,accuracy,
and standardization across the 'silver' schema .It includes checks for:
   --Null or duplicate primary keys .
   --unwanted spaces im string fields
   --Data standardization and consistency .
   --Invalid date ranges and orders.
   --Data consistency between related fields

Usage Notes:
    Run these checks after data loading silver Layer.
    Investigate and resolve any disceprencies found during checks
===================================================================
*/
---==================================================

--==============Checking 'silver.cust_crm_info===========


---Check for nulls and duplicates in primary key 
--Exceptation :No results

select cst_id,
count(*) 
from silver.crm_cust_info
group by cst_id having count(*)>1 or cst_id is null;
/* check for unwanted spaces
Exceptations:No Results
*/
select
cst_firstname
from 
silver.crm_cust_info where cst_firstname != trim(cst_firstname);

select
cst_lastname
from 
silver.crm_cust_info where cst_lastname != trim(cst_lastname)

select
cst_gndr
from 
silver.crm_cust_info where cst_gndr != trim(cst_gndr)

--data standarization and data consistency----
select distinct cst_gndr
from silver.crm_cust_info

--===================Checking 'silver.crm_prd_info==================


---Quality checks of silver layer of crm_prd_info table---
--check for nulls or duplicates of primary key --
--Exceptations:No Results---



select
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id 
having count(*)>1 or prd_id is null;

---check for unwanted Spaces--
--Exceptations:No Results--

select 
prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)


---Check for nulls or negative numbers--
--Exceptations:No Results---

select
prd_cost
from silver.crm_prd_info
where prd_cost<0 or prd_cost is null;

--Data standaridation &Consistency---
--Exceptations:No Results----
select distinct prd_line 
from silver.crm_prd_info;

---check for invalid date orders---
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

--===================Checking 'silver.crm_sales_details==================
---Quality checks of bronze layer of crm_sales_details table---


--check the order spaces ---
select 
* from bronze.crm_sales_details
where sls_ord_num !=trim(sls_ord_num)

--check sls_prd_key in crm_sales_details---
--Exceptation:No Result----
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in 
(select prd_key from silver.crm_prd_info)

--check sls_prd_key in crm_sales_details---
----expectation:No Result----

select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in 
(select cst_id from silver.crm_cust_info)


--Check for sls_order-dt should in proper order--
--check for invalid dates----
--Exceptation:No Results---

select
NULLIF(sls_order_dt,0)
from bronze.crm_sales_details
where sls_order_dt <= 0
or sls_order_dt > 20500101
or sls_order_dt <19000101

--ORDER DATE SHOULD BE EARLIER THAN SHIPING DATE---
--EXCEPTATIONS:NO RESULT---
SELECT 
* FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt

---check data consistency :between sales,quantity and price---
--->>sales=quantity*price---
-- >> values must not  be null,zero ar negativity---

SELECT DISTINCT
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,

CASE WHEN sls_sales <= 0 or sls_sales !=sls_quantity*ABS(sls_price) or sls_sales is null
      then sls_quantity*ABS(sls_price)
      ELSE sls_sales
END sls_sales,
CASE WHEN sls_price is null or sls_price <=0
     then sls_sales/nullif(sls_quantity,0)
     ELSE sls_price
END sls_price

         
from bronze.crm_sales_details
where sls_sales !=sls_quantity*sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <=0 or sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price

select  
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details



/*---Quality checks of sILVER layer of crm_sales_details table---
-CHECK FOR INVALID DATE ORDERS --
--Exceptations:No Results---*/

SELECT 
* FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt
---check data consistency :between sales,quantity and price---
--->>sales=quantity*price---
-- >> values must not  be null,zero ar negativity---
SELECT DISTINCT
sls_sales ,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales !=sls_quantity*sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <=0 or sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price

select * from silver.crm_sales_details


--===================Checking 'silver.erp_cust_az12==================


/*Identify out of range dates */
select distinct 
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate >GETDATE()
/* DAta standarisation and consistency */
select distinct
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') then 'Female'
     when UPPER(TRIM(gen)) IN ('M','Male') then 'Male'
     else 'n/a'
END AS gen
from bronze.erp_cust_az12;


/* checking the qulaity checks  of silver erp_cust_az12*/
/*Identify out of range dates */
select distinct 
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate >GETDATE()
/* DAta standarisation and consistency */
select distinct
gen
from silver.erp_cust_az12;


/* Quality Checks*/
---check wheather cid is a primary value with no nulls and duplicats
select
cid,
count(*)
from bronze.erp_cust_az12
group by cid
having count(*)>1

--===================Checking 'silver.erp_cust_az12==================

---Data standarisation and consistency---
select distinct
cntry ,
CASE WHEN TRIM(cntry) = 'DE' then 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') then 'United States'
	WHEN TRIM(cntry) = '' or cntry IS NULL  then 'n/a'
	ELSE cntry
END as cntry
from bronze.erp_loc_a101;

select distinct
cntry
from silver.erp_loc_a101

select * from silver.erp_loc_a101

--===================Checking 'silver.erp_cust_az12==================

/* check for unwanted spaces for cat and subcat and maintenance */
select
* from bronze.erp_px_cat_g1v2 where cat !=trim(cat) or subcat !=trim(subcat) or maintenance != trim(maintenance)
/* data standarization and consistency ,Here everything is clean no need of cleaning */
select distinct 
cat from bronze.erp_px_cat_g1v2
select distinct 
subcat from bronze.erp_px_cat_g1v2
select distinct 
maintenance from bronze.erp_px_cat_g1v2

/* check for unwanted spaces for cat and subcat and maintenance */
select
* from silver.erp_px_cat_g1v2 where cat !=trim(cat) or subcat !=trim(subcat) or maintenance != trim(maintenance)
/* data standarization and consistency ,Here everything is clean no need of cleaning */
select distinct 
cat from silver.erp_px_cat_g1v2
select distinct 
subcat from silver.erp_px_cat_g1v2
select distinct 
maintenance from silver.erp_px_cat_g1v2
