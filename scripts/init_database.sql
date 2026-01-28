-- Switch to the master database to create a new database
USE master;

-- Create a new database for the data warehouse
CREATE DATABASE Datawarehouse;

-- Switch context to the newly created Datawarehouse
USE Datawarehouse;

-----------------
-- Create schemas for Medallion Architecture
-----------------

-- Bronze schema: stores raw, ingested data as-is
CREATE SCHEMA bronze;
GO

-- Silver schema: stores cleaned and standardized data
CREATE SCHEMA silver;
GO

-- Gold schema: stores business-ready tables, KPIs, and analytics
CREATE SCHEMA gold;
GO

-----------------
-- Schemas setup complete
-----------------

