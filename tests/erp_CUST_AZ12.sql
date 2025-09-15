-- Brief overview of the erp_CUST_AZ12 table
SELECT TOP (1000) [cid]
      ,[bdate]
      ,[gen]
FROM [DataWarehouse].[bronze].[erp_CUST_AZ12];

-- Check for unwanted spaces or NULLs in CID
-- Expectation: No Result
SELECT *
FROM bronze.erp_CUST_AZ12
WHERE cid <> TRIM(cid) 
   OR cid <> REPLACE(TRIM(cid),' ','')
   OR cid IS NULL;

-- Check for validity of Birth Dates (no birth date should be in the future)
SELECT *
FROM bronze.erp_CUST_AZ12
WHERE DATEDIFF(YEAR,bdate,GETDATE()) > 100 
   OR bdate > GETDATE();

SELECT DISTINCT gen
FROM bronze.erp_CUST_AZ12;

-- Insert data into Silver layer table after:
-- 1) Data Cleaning, Data Validation
-- 2) Data Normalisation
TRUNCATE TABLE silver.erp_CUST_AZ12;
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

