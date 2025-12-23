/*
======================================================================================
Stored Procedure:Load Bronze Layer(Source->Bronze)
Script Purpose:
This stored procedure loads data into the 'bronze' scehma from external csv files.
It perform the following actions
--truncates the bronze  tables before loading data.
--uses the 'bulk insert' command to load datafrom csv files to bronze tables
Parameters:None
usage example:
exec bronze.load_bronze

*/

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

