/*
===============================================================================
       Stored Procedure: Bulk inserts data into tables in bronze schema
===============================================================================

Script Purpose: this stored procedure loads data into bronze schema from external
				csv files. This performs the following actions:
				- truncates the bronze tables before loading data
				- used 'BULK INSERT' command to load data into empty tables

Parameters: None (this stored procedure does not accept any parameters)

Usage example: 
	EXEC bronze.load_bronze;
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch DATETIME, @end_batch DATETIME;
	SET @start_batch = GETDATE();
	BEGIN TRY
		PRINT '==============================================================';
		PRINT '                  Loading Bronze Layer';
		PRINT '==============================================================';

		PRINT '--------------------------------------------------------------';
		PRINT '                  Loading CRM Tables';
		PRINT '--------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; --First truncate the existing table, before loading entire data again
		PRINT '>> Inserting Data into: crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, --Skip the first row from csv file during insert as it contain column names
			FIELDTERMINATOR = ',', --Specify the separator type
			TABLOCK --Lock the table
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT'..............................................................';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into: crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT'..............................................................';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';

		PRINT '--------------------------------------------------------------';
		PRINT '                  Loading ERP Tables';
		PRINT '--------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT '>> Inserting Data into: erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT'..............................................................';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT '>> Inserting Data into: erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT'..............................................................';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data into: erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT'..............................................................';
	END TRY
	BEGIN CATCH
		PRINT '=============================================================';
		PRINT '       ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT '=============================================================';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH
	SET @end_batch = GETDATE();
	PRINT 'TOTAL PROCESSING TIME: ' + CAST (DATEDIFF(MILLISECOND, @start_batch,@end_batch) AS NVARCHAR) + ' milliseconds';
END
