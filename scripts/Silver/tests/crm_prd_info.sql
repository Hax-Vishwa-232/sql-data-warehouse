-- Brief overview of the crm_prd_info table
SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
FROM [DataWarehouse].[bronze].[crm_prd_info];

-- Check for NULLs or duplicates in Primary Key
-- Expectation: No result
SELECT prd_id,
    COUNT(*) As count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for duplicate rows in Table
-- Expectation: No result
SELECT prd_key,
    COUNT(*) AS count
FROM bronze.crm_prd_info
GROUP BY prd_key
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
HAVING COUNT(*) > 1;

-- Visual overview of combination of prd_key and prd_num (might help in visually detecting duplicates)
SELECT 
    m.prd_key, 
    m.prd_nm, 
    COUNT(*) AS count
FROM bronze.crm_prd_info AS m
GROUP BY m.prd_key, m.prd_nm
ORDER BY m.prd_key, m.prd_nm;

-- Observation: All prd_key values does not contain any spaces in names
-- Check Whether there are spaces in any data point of prd_key field
-- Expectation: No Result
SELECT *
FROM bronze.crm_prd_info
WHERE TRIM(prd_key) LIKE '% %' OR prd_key <> UPPER(prd_key);

-- Detect Unclean data points in prd_nm
-- Expectation: No Result
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

---------------------------------------------------------------------------------------------------------------------
-- Detecting Data points in prd_nm that might get counted distinctly due to presence of irrelevant spaces bw names --
---------------------------------------------------------------------------------------------------------------------

SELECT 
    COUNT(DISTINCT REPLACE(prd_nm,' ','')) AS count
FROM bronze.crm_prd_info

SELECT
    COUNT(DISTINCT TRIM(prd_nm)) AS count
FROM bronze.crm_prd_info
-- Expectation: Count from both above queries should be same

SELECT REPLACE(prd_nm, ' ', '') AS temp_prd_nm,
    COUNT(*) count
FROM (
    SELECT DISTINCT TRIM(prd_nm) AS prd_nm 
    FROM bronze.crm_prd_info
) t
GROUP BY REPLACE(prd_nm, ' ', '')
HAVING COUNT(*) > 1;

---------------------------------------------------------------------------------------------------------------------

-- Check whether prd_cost is negative or NULL
-- Expectation: No Result
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info ;

-- Check whether any prd_start_dt date is after the prd_end_dt.
-- Expectation: No Result
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt OR prd_start_dt IS NULL;

-- Insert data into Silver layer table after:
-- 1) Removing Duplicates, Unwanted spaces
-- 2) Performing data normalisation, data enrichment
-- 3) Deriving required columns for next stages of transformation
TRUNCATE TABLE silver.crm_prd_info;
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
