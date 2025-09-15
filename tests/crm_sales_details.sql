-- Brief overview of the crm_sales_details table
SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
FROM [DataWarehouse].[bronze].[crm_sales_details];

-- Check for any errors in first 3 String Columns
-- Expectation: No result
SELECT sls_ord_num,
       sls_prd_key,
       sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num) 
   OR sls_prd_key <> TRIM(sls_prd_key) 
   OR sls_prd_key <> REPLACE(TRIM(sls_prd_key), ' ','');

-- checking the correctness of Date fields 
-- Expectation: No result
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
   OR LEN(sls_order_dt) <> 8
   OR sls_order_dt < 19000101 
   OR sls_order_dt > 20500101;

-- checking the correctness of Date fields 
-- Expectation: No result
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
  OR sls_order_dt > sls_due_dt;

-- Checking for incorrect or missing sales, quantity and price data
-- Expectation: No result
SELECT sls_sales 
      ,sls_quantity 
      ,sls_price 
FROM bronze.crm_sales_details
WHERE sls_sales < 0 OR sls_sales IS NULL
   OR sls_quantity < 0 OR sls_quantity IS NULL
   OR sls_price < 0 OR sls_price IS NULL
   OR sls_price * sls_quantity <> sls_sales
ORDER BY sls_sales, sls_quantity, sls_price;

-- Handling incorrect data using following rules
-- Rules: 1) If sales is negative, zero or NULL, derive it using price
--        2) If price is zero or NULL, calculate using sales and quantity
--        3) If price is negative, convert it to a positive value 
SELECT sls_sales AS old_sales
       ,sls_quantity AS old_quantity
       ,sls_price AS old_price
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
FROM bronze.crm_sales_details
WHERE sls_sales < 0 OR sls_sales IS NULL
   OR sls_quantity < 0 OR sls_quantity IS NULL
   OR sls_price < 0 OR sls_price IS NULL
   OR sls_price * sls_quantity <> sls_sales
ORDER BY sls_sales, sls_quantity, sls_price;

-- Insert data into Silver layer table after:
-- 1) Handling incorrect data, Data type casting
-- 2) Resolving data consistency issues, but checking for missing data, invalid data, by deriving columns from already existing ones
TRUNCATE TABLE silver.crm_sales_details;
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
