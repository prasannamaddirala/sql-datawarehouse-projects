
/*
======================================================================================
Stored Procedure:Load silver Layer(Source->silver)
Script Purpose:
This stored procedure loads data into the 'bronze' scehma from external csv files.
It perform the following actions
--truncates the silver  tables before loading data.
--uses the 'bulk insert' command to load datafrom csv files to bronze tables
Parameters:None
usage example:
exec silver.load_silver

*/




EXEC silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME ,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=======================================';
		PRINT 'Loading Silver Layer';
		PRINT '========================================';

		PRINT '---------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '---------------------------------------';

		--lOADING silver crm_cust_info---
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE: silver crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>iNSERTING DATA INTO: silver.crm_cust_info' ;
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
		select
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status) )='S' THEN 'Female'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr) )='F' THEN 'Female'
			 WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		from
		(select 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) AS flag_last
		from bronze.crm_cust_info WHERE CST_ID IS NOT NULL)t 
		where flag_last = 1 ;
		SET @end_time=GETDATE();
		PRINT '>>lOAD DURATION  '+ CAST(DATEDIFF('SECOND',@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT '>>-------------------';

		--lOADING silver crm_prd_info---
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE: silver crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>iNSERTING DATA INTO: silver.crm_prd_info'; 
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT
		prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id,---Extract category id 
		SUBSTRING(prd_key,7,len(prd_key)) as prd_key,--Extract Product Key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
			 ELSE 'n/a' 
			 END as prd_line,--Map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt asc)-1 as prd_end_dt --calculate end date as one day before the next start date
		from bronze.crm_prd_info;
		SET @end_time=GETDATE();
		PRINT '>>lOAD DURATION  '+ CAST(DATEDIFF('SECOND',@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT '>>-------------------';

		--lOADING silver crm_sales_details---
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE: silver crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>iNSERTING DATA INTO: silver.crm_sales_details'; 
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE when sls_order_dt = 0 or len(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END   sls_order_dt,   

			CASE when sls_ship_dt = 0 or len(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END   sls_ship_dt, 

			CASE when sls_due_dt = 0 or len(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END   sls_due_dt, 
			CASE WHEN sls_sales <= 0 or sls_sales !=sls_quantity*ABS(sls_price) or sls_sales is null
				  then sls_quantity*ABS(sls_price)
				  ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price is null or sls_price <=0
				 then sls_sales/nullif(sls_quantity,0)
				 ELSE sls_price
			END sls_price
		from bronze.crm_sales_details;
		SET @end_time=GETDATE();
		PRINT '>>lOAD DURATION  '+ CAST(DATEDIFF('SECOND',@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT '>>-------------------';

		--lOADING silver erp_cust_az12---
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE: silver erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>iNSERTING DATA INTO: silver. erp_cust_az12'; 
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		select 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			 else cid
		END as cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END as  bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') then 'Female'
			 when UPPER(TRIM(gen)) IN ('M','Male') then 'Male'
			 else 'n/a'
		END AS gen
		from bronze.erp_cust_az12;
		SET @end_time=GETDATE();
		PRINT '>>lOAD DURATION  '+ CAST(DATEDIFF('SECOND',@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT '>>-------------------';

		--lOADING silver erp_loc_a101---
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE: silver erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>iNSERTING DATA INTO: silver.erp_loc_a101'; 
		INSERT INTO silver.erp_loc_a101
		(cid,cntry)
		SELECT
		replace(cid,'-','') as cid,
		CASE WHEN TRIM(cntry) = 'DE' then 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') then 'United States'
			WHEN TRIM(cntry) = '' or cntry IS NULL  then 'n/a'
			ELSE cntry
		END as cntry
		from bronze.erp_loc_a101;
		SET @end_time=GETDATE();
		PRINT '>>lOAD DURATION  '+ CAST(DATEDIFF('SECOND',@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT '>>-------------------';

		--lOADING silver erp_px_cat_g1v2---
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLE: silver erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>INSERTING DATA INTO: silver.erp_px_cat_g1v2'; 
		insert into silver.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time=GETDATE();
		PRINT '>>lOAD DURATION  '+ CAST(DATEDIFF('SECOND',@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT '>>-------------------';

		SET @batch_end_time=GETDATE();
		PRINT '=================================';
		PRINT 'Loading silver Layer is completed';
		PRINT '--Total Duration :'+CAST(DATEDIFF('SECOND',@batch_start_time,@batch_end_time) AS NVARCHAR) + 'Seconds';
		PRINT '=================================='
	END TRY
	BEGIN CATCH
	PRINT '========================================';
	PRINT 'ERROR OCCURED DURINF LOADING BRONZE LAYER';
	PRINT 'Error message '+ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE '+CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE '+CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '========================================='

	END CATCH

END
