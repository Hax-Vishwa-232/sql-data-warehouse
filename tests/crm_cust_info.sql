-- Brief overview of the crm_cust_info table
SELECT TOP (1000) [cst_id]
   ,[cst_key]
   ,[cst_firstname]
   ,[cst_lastname]
   ,[cst_marital_status]
   ,[cst_gndr]
   ,[cst_create_date]
FROM [DataWarehouse].[bronze].[crm_cust_info];

-- Check for NULLs or duplicates in Primary Key
-- Expectation: No result
SELECT 
   cst_id,
   COUNT(*) count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- identify the reason for duplication
-- identify the condition according to which duplicate rows needs to be removed
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29449;

-- Check for unwanted spaces in name
-- Expectation: No result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

-- Check all distinct values of cst_marital_status or cst_gndr
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

-- Insert data into Silver layer table after:
-- 1) Removing Duplicates, Unwanted spaces
-- 2) Performing data normalisation
TRUNCATE TABLE silver.crm_cust_info;
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
