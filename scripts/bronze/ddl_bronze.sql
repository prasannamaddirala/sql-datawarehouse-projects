/*
==========================================================================
DDL Script:create bronze  tables
===========================================================================
Script Purpose:
This script creates tables in the bronze schema,dropping existing tables
if they already exists
Run this script to redifine the DDL structure of bronze tables
===========================================================================

*/
IF OBJECT_ID('bronze.crm_cust_info','u') IS NOT NULL
   DROP TABLE bronze.crm_cust_info;

create table bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
IF OBJECT_ID('bronze.crm_prd_info','u') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
IF OBJECT_ID('bronze.crm_sales_details','u') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
IF OBJECT_ID('bronze.erp_cust_az12','u') IS NOT NULL
   DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
IF OBJECT_ID('bronze.erp_loc_a101','u') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)      
);
IF OBJECT_ID('bronze.erp_px_cat_g1v2','u') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
	id		     NVARCHAR(50),
	cat		     NVARCHAR(50),
	subcat	     NVARCHAR(50),
	maintenance  NVARCHAR(50)
);
/* Delete all the rows of the table*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
  BEGIN TRY
    SET @@batch_start_time=GETDATE();
	PRINT '==================================';
	PRINT 'Loading Bronze Layer';
	PRINT '==================================';

	PRINT '===================================';
	PRINT 'lOADING crm TABLES';
	PRINT '====================================';

	SET @start_time=GETDATE();
	PRINT '>> TRUNCATING TABLE :bronze_crm_cust_info';
	truncate table bronze.crm_cust_info;
	PRINT '>> INSERTING DATA INTO :bronze_crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\mpras\OneDrive\Desktop\python documents\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	WITH(
		FIRSTROW=2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time=GETDATE();
	PRINT 'Load Duration'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print '>>-------------------------------'


	SET @start_time=GETDATE();
	PRINT '>> TRUNCATING TABLE :crm_prd_info';
	truncate table bronze.crm_prd_info;
	PRINT '>> INSERTING DATA INTO :bronze_crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\mpras\OneDrive\Desktop\python documents\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCk
	);
    SET @end_time=GETDATE();
	PRINT 'Load Duration'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print '>>-------------------------------'


    SET @start_time=GETDATE();
	PRINT '>> TRUNCATING TABLE :crm_sales_details';
	truncate table bronze.crm_sales_details;
	PRINT '>> INSERTING DATA INTO :bronzecrm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\mpras\OneDrive\Desktop\python documents\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCk
	);
	SET @end_time=GETDATE();
	PRINT 'Load Duration'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print '>>-------------------------------'


	PRINT '==================================='
	PRINT 'lOADING ERP TABLES'
	PRINT '===================================='
    SET @start_time=GETDATE();
	PRINT '>> TRUNCATING TABLE :erp_cust_az12';
	truncate table bronze.erp_cust_az12;
	PRINT '>> INSERTING DATA INTO :bronze_erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\mpras\OneDrive\Desktop\python documents\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCk
	);
	SET @end_time=GETDATE();
	PRINT 'Load Duration'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print '>>-------------------------------'

	SET @start_time=GETDATE();
	PRINT '>> TRUNCATING TABLE :erp_loc_a101';
	truncate table bronze.erp_loc_a101;
	PRINT '>> INSERTING DATA INTO :bronze_erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\mpras\OneDrive\Desktop\python documents\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCk
	);
	SET @end_time=GETDATE();
	PRINT 'Load Duration'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print '>>-------------------------------'


	SET @start_time=GETDATE();
	PRINT '>> TRUNCATING TABLE :erp_px_cat_g1v2';
	truncate table bronze.erp_px_cat_g1v2;
	PRINT '>> INSERTING DATA INTO :erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\mpras\OneDrive\Desktop\python documents\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
	with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCk
	);
	SET @end_time=GETDATE();
	PRINT 'Load Duration'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	print '>>-------------------------------'
	SET @batch_end_time=GETDATE();
	PRINT '===================================';
	PRINT 'Loading Bronze Layer is Completed';
	PRINT '-Total Load Duration -'+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR)+'SECONDS';
	PRINT '===================================';

	END TRY
	BEGIN CATCH
		PRINT '================================';
		PRINT 'ERROR OCCURED DURING BRONZE LAYER';
		PRINT 'Error Message'+ERROR_MESSAGE();
		PRINT 'Error Number'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State'+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================';

	END CATCH
END
