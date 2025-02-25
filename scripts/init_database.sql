/*
================================================================================
Create Database and Schemas
================================================================================

Script Purpose : This script create new database named 'DataWarehouse' after checking it's existence.
				 If it exists then it's dropped and recreated. Also, this script sets up 3 schemas under
				 the same database: 'bronze','silver' & 'gold'

WARNING!
	Running this script will drop the entire 'DataWarehouse' database if it exists.
*/

--using master DB
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET  SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--Creating Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

--Switching to new DB
USE DataWarehouse;
GO

--Creating Schemas inside the DB according to the architecture(Bronze,Silver & Gold)
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
