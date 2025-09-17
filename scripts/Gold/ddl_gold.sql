/* 
======================================================================================
						DDL Script: Create Gold views
======================================================================================

Script Purpose: 
	This script creates views for the Gold layer in data warehouse.
	The Gold layer represents the final dimension and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver Layer
	to produce a clean, enriched, and business ready dataset.

Usage: These views can be queried directly for analytics and reporting
*/

--=========================================================================
-- Create Dimension: gold.dim_customers
--=========================================================================
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	CASE
		WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender, -- CRM is master for gender info
	cl.cntry AS country,
	ci.cst_marital_status AS martial_status,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date	
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_CUST_AZ12 AS ca
ON ca.cid = ci.cst_key
LEFT JOIN silver.erp_LOC_A101 AS cl
ON cl.cid = ci.cst_key
GO

--=========================================================================
-- Create Dimension: gold.dim_products
--=========================================================================
CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY p.prd_start_dt, p.prd_key) AS product_key, -- surrogate key
	p.prd_id AS product_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	p.prd_line AS product_line,
	p.prd_cost AS cost,
	p.prd_start_dt AS start_date	
FROM silver.crm_prd_info AS p
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pc.id = p.cat_id
WHERE p.prd_end_dt IS NULL -- filter out all historical data
GO

--=========================================================================
-- Create fact: gold.fact_sales
--=========================================================================
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
	s.sls_ord_num AS order_number,	-- surrogate key from dimension
	p.product_key,					-- surrogate key from dimension
	c.customer_key,					-- surrogate key from dimension
	s.sls_order_dt AS order_date,	-- Date
	s.sls_ship_dt AS shipping_date,	-- Date
	s.sls_due_dt AS due_date,		-- Date
	s.sls_price AS price,			-- Measure
	s.sls_quantity AS quantity,		-- Measure
	s.sls_sales AS sales_amount		-- Measure
FROM silver.crm_sales_details AS s
LEFT JOIN gold.dim_customers AS c
ON c.customer_id = s.sls_cust_id
LEFT JOIN gold.dim_products AS p
ON p.product_number = s.sls_prd_key
GO
