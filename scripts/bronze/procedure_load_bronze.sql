/*
================================================================
Stored Procedure: Load Bronze Layer (Source >> Bronze Layer)
================================================================
Script Purpose: This Stored Procedure loads data from the 'source'(external csv files) into the 'bronze layer' tables.
				Truncates the bronze tables and then load the data from csv files using 'BULK INSERT'.
				Parameters: None
*/

EXEC bronze.load_bronze;
-- creating stored procedure
CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@bronze_start_time DATETIME,@bronze_end_time DATETIME;
	BEGIN TRY
		PRINT '***Loading Bronze Layer***';

		PRINT '----------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------';
		SET @bronze_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> Truncating the Table: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting into the Table: bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Mahesh N S\Desktop\Microsoft SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating the Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting into the Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Mahesh N S\Desktop\Microsoft SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating the Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting into the Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Mahesh N S\Desktop\Microsoft SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'----------------------------------------------------------------------------';

		PRINT '----------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating the Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> Inserting into the Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Mahesh N S\Desktop\Microsoft SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating the Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting into the Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Mahesh N S\Desktop\Microsoft SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating the Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting into the Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Mahesh N S\Desktop\Microsoft SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'----------------------------------------------------------------------------';
		SET @bronze_end_time = GETDATE();
		PRINT 'Total Bronze Layer Load Time: ' + CAST(DATEDIFF(SECOND,@bronze_start_time,@bronze_end_time) AS NVARCHAR) + 'Seconds';
	END TRY
	BEGIN CATCH
			PRINT ('============================================================');
			PRINT ('Error occured!');
			PRINT ('Error Message:' + error_message());
			PRINT ('Error number:' + cast(error_number() as nvarchar));
			PRINT ('Error line:' + cast(error_line() as nvarchar));
			PRINT ('Error procedure:' + error_procedure());
			PRINT ('============================================================');
	END CATCH
END

