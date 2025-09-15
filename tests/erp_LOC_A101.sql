-- Brief overview of the erp_LOC_A101 table
SELECT TOP (1000) [cid]
      ,[cntry]
FROM [DataWarehouse].[bronze].[erp_LOC_A101]

-- Remove '-' from cid field to match with cst_key field in crm_cust_info table, then compare the fields from both tables
-- Expectation: No Result
SELECT REPLACE(REPLACE(TRIM(cid),' ',''), '-','') AS cid
FROM bronze.erp_LOC_A101
WHERE REPLACE(REPLACE(TRIM(cid),' ',''), '-','') NOT IN (
    SELECT cst_key
    FROM silver.crm_cust_info
)

-- Check and remove Data Consistency issues from cntry field
SELECT DISTINCT 
    cntry,
    CASE 
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_LOC_A101


-- Insert data into Silver layer table after:
-- 1) Data Validation
-- 2) Checking for Data Consistency
TRUNCATE TABLE silver.erp_LOC_A101;
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
