/*
===============================================================================================
       Stored Procedure: Inserts data into tables in Silver schema (Bronze -> Silver)
===============================================================================================

Script Purpose: this stored procedure loads data into Silver schema from Bronze Schema
				after Data Transformation. This performs the following actions:
				- truncates the Silver tables before loading data
				- Performs various Data Transformation Operations on Tables before
                  loading them to Silver Layer.

** User Note: Some Columns in Tables might require additional manual Cleaning Operations
              (like prd_nm in crm_prd_info table). Some of predicted errors and 
              corresponding operations on such columns are listed in Cleaning Operations' Files.
              Kindly refer them for Reference.

Parameters: None (this stored procedure does not accept any parameters)

Usage example: 
	EXEC silver.load_silver;
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch DATETIME, @end_batch DATETIME;
    SET @start_batch = GETDATE();
    BEGIN TRY

        PRINT '==========================================================================================';
        PRINT '                             Loading Silver Layer';
        PRINT '==========================================================================================';

        PRINT '------------------------------------------------------------------------------------------';
        PRINT '                             Loading CRM Tables';
        PRINT '------------------------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data into: crm_cust_info';
        INSERT INTO silver.crm_cust_info (
	        cst_id
           ,cst_key
           ,cst_firstname
           ,cst_lastname
           ,cst_marital_status
           ,cst_gndr
           ,cst_create_date
        )	
        SELECT cst_id
           ,cst_key
           ,TRIM(cst_firstname) AS cst_firstname
           ,TRIM(cst_lastname) AS cst_lastname
           ,CASE 
		        WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
		        WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
		        ELSE 'n/a'
	        END AS cst_marital_status
           ,CASE 
		        WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
		        WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
		        ELSE 'n/a'
	        END AS cst_gndr
           ,cst_create_date
        FROM (
	        SELECT *,
		        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_latest
	        FROM bronze.crm_cust_info
	        WHERE cst_id IS NOT NULL
        ) t WHERE flag_latest = 1;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS VARCHAR) + ' milliseconds';
        PRINT '..........................................................................................';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data into: crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,			
	        cat_id,				
	        prd_key,			
	        prd_nm,				
	        prd_cost,			
	        prd_line,			
	        prd_start_dt,		
	        prd_end_dt
        ) 
        SELECT prd_id
              ,REPLACE(SUBSTRING(REPLACE(TRIM(prd_key),' ',''), 1, 5), '-','_') AS cat_id
              ,SUBSTRING(REPLACE(TRIM(prd_key),' ',''), 7, LEN(prd_key)) AS prd_key
              ,TRIM(prd_nm) AS prd_nm
              ,ISNULL(prd_cost, 0) AS prd_cost
              ,CASE TRIM(UPPER(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
                END AS prd_line
              ,CAST(prd_start_dt AS DATE) AS prd_start_dt
              ,CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE)  AS prd_end_dt
        FROM bronze.crm_prd_info
        ORDER BY prd_id;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS VARCHAR) + ' milliseconds';
        PRINT '..........................................................................................';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data into: crm_sales_details';
        INSERT INTO silver.crm_sales_details (
               sls_ord_num
              ,sls_prd_key
              ,sls_cust_id
              ,sls_order_dt
              ,sls_ship_dt
              ,sls_due_dt
              ,sls_sales
              ,sls_quantity
              ,sls_price
        )
        SELECT TRIM(sls_ord_num) AS sls_ord_num
              ,REPLACE(TRIM(sls_prd_key), ' ','') AS sls_prd_key
              ,sls_cust_id
              ,CASE 
                    WHEN sls_order_dt <= 0 
                        OR LEN(sls_order_dt) <> 8
                        OR sls_order_dt < 19000101 
                        OR sls_order_dt > 20500101 THEN NULL
                    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                END AS sls_order_dt
              ,CASE 
                    WHEN sls_ship_dt <= 0 
                        OR LEN(sls_ship_dt) <> 8
                        OR sls_ship_dt < 19000101 
                        OR sls_ship_dt > 20500101 THEN NULL
                    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                END AS sls_ship_dt
              ,CASE 
                    WHEN sls_due_dt <= 0 
                        OR LEN(sls_due_dt) <> 8
                        OR sls_due_dt < 19000101 
                        OR sls_due_dt > 20500101 THEN NULL
                    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
                END AS sls_due_dt
              ,CASE
                    WHEN sls_sales IS NULL 
                      OR sls_sales  <= 0 
                      OR sls_sales <> sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                END AS sls_sales
              ,sls_quantity
              ,CASE
                    WHEN sls_price IS NULL 
                      OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
                    ELSE sls_price
                END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS VARCHAR) + ' milliseconds';
        PRINT '..........................................................................................';

        PRINT '------------------------------------------------------------------------------------------';
        PRINT '                             Loading CRM Tables';
        PRINT '------------------------------------------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: erp_CUST_AZ12';
        TRUNCATE TABLE silver.erp_CUST_AZ12;
        PRINT '>> Inserting Data into: erp_CUST_AZ12';
        INSERT INTO silver.erp_CUST_AZ12 (
             cid
            ,bdate
            ,gen
        ) 
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid)) 
                ELSE TRIM(cid)
            END AS cid
           ,CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate
           ,CASE
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_CUST_AZ12;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS VARCHAR) + ' milliseconds';
        PRINT '..........................................................................................';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: erp_LOC_A101';
        TRUNCATE TABLE silver.erp_LOC_A101;
        PRINT '>> Inserting Data into: erp_LOC_A101';
        INSERT INTO silver.erp_LOC_A101 (
            cid,
            cntry
        )
        SELECT 
            REPLACE(REPLACE(TRIM(cid),' ',''), '-','') AS cid
           ,CASE 
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_LOC_A101;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS VARCHAR) + ' milliseconds';
        PRINT '..........................................................................................';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: erp_PX_CAT_G1V2';
        TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
        PRINT '>> Inserting Data into: erp_PX_CAT_G1V2';
        INSERT INTO silver.erp_PX_CAT_G1V2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            TRIM(id) AS id,
            TRIM(cat) AS cat,
            TRIM(subcat) AS subcat,
            TRIM(maintenance) AS maintenance
        FROM bronze.erp_PX_CAT_G1V2;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS VARCHAR) + ' milliseconds';
        PRINT '..........................................................................................';
    END TRY
    BEGIN CATCH
        PRINT '==========================================================================================';
        PRINT '                     ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT '==========================================================================================';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS VARCHAR);
    END CATCH
    SET @end_batch = GETDATE();
	PRINT 'TOTAL PROCESSING TIME: ' + CAST(DATEDIFF(MILLISECOND, @start_batch, @end_batch) AS NVARCHAR) + ' milliseconds';
END
