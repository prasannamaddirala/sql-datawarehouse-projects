/*create database and schemas 
script purpose:
This script create new database named 'DataWarehouse' and creates three schemas 'bronze', 'silver', 'gold' 
*/

USE master;
Go 


/* Create the 'DataWarehouse' database   */
CREATE DATABASE DataWarehouse;
GO


use DataWarehouse
Go

/*Create schemas*/
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO



CREATE SCHEMA gold;
GO
