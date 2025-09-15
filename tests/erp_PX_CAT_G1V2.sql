-- Brief overview of the erp_PX_CAT_G1V2 table
SELECT TOP (1000) [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
FROM [DataWarehouse].[bronze].[erp_PX_CAT_G1V2]

-- Check for any Data Consistency issues
SELECT DISTINCT cat
FROM bronze.erp_PX_CAT_G1V2

SELECT DISTINCT subcat
FROM bronze.erp_PX_CAT_G1V2

SELECT DISTINCT maintenance
FROM bronze.erp_PX_CAT_G1V2

-- Insert data into Silver layer table
TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
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
FROM bronze.erp_PX_CAT_G1V2
