/* 
===========================================================================

CREATE DATABASE AND SCHEMAS

===========================================================================

Script Purpose: this script creates a new database 'DataWarehouse' after checking
				if it already exists. If it exists, drop the existing and create it
				from scratch. The scipt additionally creates 3 new schemas named
				'bronze','silver','gold' within the database

! WARNING !: this script drops the existing database and creates the same database from 
			 scratch. So, any data stored previously within the database will be completely
			 removed when the script is run. Proceed with caution and make sure to have 
			 proper backup before running the script

*/

USE master;
GO

-- check whether database 'DataWarehouse' already exists. If yes, drop it
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Recreate Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- create schemas in DataWarehouse
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
