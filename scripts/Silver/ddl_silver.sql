/*
===========================================================
             DDL Script: Create Silver Tables
===========================================================

Script Purpose: this script creates tables in 'Silver' schema of DataWarehouse Database.
				Drops already existing tables and creates all tables from scratch when 
				script is run.

*/


USE DataWarehouse;
GO

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id				int,
	cst_key			    NVARCHAR(50),
	cst_firstname	    NVARCHAR(20),
	cst_lastname	    NVARCHAR(MAX),
	cst_marital_status	NVARCHAR(10),
	cst_gndr		    NVARCHAR(10),
	cst_create_date	  	DATE,
	dwh_create_date     DATETIME2 DEFAULT GETDATE() --METADATA COLUMN
);
GO

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id				int,
	cat_id				NVARCHAR(10),
	prd_key				NVARCHAR(MAX),
	prd_nm				NVARCHAR(MAX),
	prd_cost			INT,
	prd_line			NVARCHAR(20),
	prd_start_dt		DATE,
	prd_end_dt			DATE,
	dwh_create_date     DATETIME2 DEFAULT GETDATE() --METADATA COLUMN
);
GO

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num			NVARCHAR(50),
	sls_prd_key			NVARCHAR(50),
	sls_cust_id			INT,
	sls_order_dt		DATE,
	sls_ship_dt			DATE,
	sls_due_dt			DATE,
	sls_sales			INT,
	sls_quantity		INT,
	sls_price			INT,
	dwh_create_date		DATETIME2 DEFAULT GETDATE() --METADATA COLUMN
);
GO 								

IF OBJECT_ID ('silver.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12 (
	cid				NVARCHAR(50),
	bdate			DATE,
	gen				NVARCHAR(20),
	dwh_create_date DATETIME2 DEFAULT GETDATE() --METADATA COLUMN
);
GO 		

IF OBJECT_ID ('silver.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101 (
	cid				NVARCHAR(50),
	cntry			NVARCHAR(20),
	dwh_create_date DATETIME2 DEFAULT GETDATE() --METADATA COLUMN
);
GO 	

IF OBJECT_ID ('silver.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2 (
	id			  	NVARCHAR(50),
	cat			  	NVARCHAR(20),
	subcat		  	NVARCHAR(50),
	maintenance	  	NVARCHAR(10),
	dwh_create_date DATETIME2 DEFAULT GETDATE() --METADATA COLUMN
);
GO 			


