/*
===========================================================
             DDL Script: Create Bronze Tables
===========================================================

Script Purpose: this script creates tables in 'Bronze' schema of DataWarehouse Database
				Drops already existing tables and creates all tables from scratch when 
				script is run.

*/


USE DataWarehouse;
GO

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id				int,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(20),
	cst_lastname		NVARCHAR(MAX),
	cst_marital_status	NVARCHAR(10),
	cst_gndr			NVARCHAR(10),
	cst_create_date		DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id				int,
	prd_key				NVARCHAR(MAX),
	prd_nm				NVARCHAR(MAX),
	prd_cost			INT,
	prd_line			NVARCHAR(10),
	prd_start_dt		DATETIME,
	prd_end_dt			DATETIME
);
GO

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num				NVARCHAR(50),
	sls_prd_key				NVARCHAR(50),
	sls_cust_id				INT,
	sls_order_dt			BIGINT,
	sls_ship_dt				BIGINT,
	sls_due_dt				BIGINT,
	sls_sales				INT,
	sls_quantity			INT,
	sls_price				INT
);
GO 								

IF OBJECT_ID ('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12 (
	cid				NVARCHAR(50),
	bdate			DATE,
	gen 			NVARCHAR(20),
);
GO 		

IF OBJECT_ID ('bronze.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101 (
	cid				NVARCHAR(50),
	cntry			NVARCHAR(20),
);
GO 	

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2 (
	id				NVARCHAR(50),
	cat				NVARCHAR(20),
	subcat			NVARCHAR(50),
	maintenance		NVARCHAR(10)
);
GO 			



