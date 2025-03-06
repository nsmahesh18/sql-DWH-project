/*
=================================================================
Stored Procedure: Load Silver Layer (Bronze Layer >> Silver Layer)
=================================================================
Script Purpose: This Stored Procedure loads data from the 'Bronze Layer' into the 'Silver layer' tables.
				Truncates the Silver tables and then load the data from Bronze tables.
				Parameters: None
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
        SET @start_time = GETDATE();
	-- Data cleanup, standardization of bronze.crm_cust_info table and inserting data to silver.crm_cust_info table
	PRINT '>> Truncating the silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data into silver.crm_cust_info';

	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,	
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	SELECT 
		  cst_id,
		  cst_key,
		  TRIM(cst_firstname),	--Avoiding empty spaces 
		  TRIM(cst_lastname),	----Avoiding empty spaces
		  CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			   WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			   ELSE 'n/a'
		  END AS cst_marital_status,	-- converting abbreviated terms to its full form
		  CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			   WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			   ELSE 'n/a'
		  END AS cst_gndr, -- converting abbreviated terms to its full form
		  cst_create_date
		FROM(
			SELECT *,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag     --removing duplicates and nulls from cst_id
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL)t
		WHERE flag = 1
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'Seconds';
		PRINT'----------------------------------------------------------------------------';


	-- Data cleanup, standardization of bronze.crm_prd_info table and inserting data to silver.crm_prd_info table
	SET @start_time = GETDATE();
	PRINT '>> Truncating the silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data into silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id,		--Obtaining cat_id from prd_key to join this table to the silver.erp_px_cat_g1v2
		SUBSTRING(prd_key,7,len(prd_key)) prd_key,	--using remaining prd_key to join this table to the silver.crm_sales_details
		prd_nm,
		ISNULL (prd_cost,0) AS prd_cost,		-- replacing nulls with 0 in the prd_cost column
		CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
		END AS prd_line,	-- converting abbreviated terms to its full form
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 AS DATE) as prd_end_dt
	  FROM bronze.crm_prd_info
									/* Note : since we have end date less than start date, we're gonna replace current end dates with the next start date-1
											  which is larger than the current start date*/
	  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		



	-- Data cleanup, standardization of bronze.crm_sales_details table and inserting data to silver.crm_sales_details table

	SET @start_time = GETDATE();
	PRINT '>> Truncating the silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data into silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details(
		sls_ord_num, 
		sls_prd_key, 
		sls_cust_id,
		sls_order_dt, 
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)

	SELECT sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0  OR LEN(sls_order_dt) !=8	--Correcting the date columns 
				THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0  OR LEN(sls_ship_dt) !=8	--Correcting the date columns (No issue but just in case if anything pops up in future)
				THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0  OR LEN(sls_due_dt) !=8	--Correcting the date columns(No issue but just in case if anything pops up in future) 
				THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			--Data consistency between sales,quantity & price. Values musto not be null, zero or negative. Sales = quantity* price
			CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price <=0 OR sls_price IS NULL
				THEN sls_sales/NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
	  FROM bronze.crm_sales_details
	  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

	 -- Data cleanup, standardization of bronze.erp_cust_az12 table and inserting data to silver.erp_cust_az12 table
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating the silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data into silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)

	SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
			END AS cid,
		  CASE WHEN bdate > GETDATE() THEN NULL
			   ELSE bdate
		  END AS bdate,				--Setting future birthdates to null
		  CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			   WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			   ELSE 'n/a'
		  END AS gen			-- Normalizing Gender
	  FROM bronze.erp_cust_az12
	  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

	-- Data cleanup, standardization of bronze.erp_loc_a101 table and inserting data to silver.erp_loc_a101 table

	SET @start_time = GETDATE();
	PRINT '>> Truncating the silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data into silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
	)

	SELECT REPLACE(cid,'-','') as cid,			--removing - from cid
			CASE WHEN UPPER(TRIM(cntry)) in ('US','USA') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
				ELSE cntry
			END AS cntry			--Normalizing Country column
	  FROM bronze.erp_loc_a101
	  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


	-- Data cleanup, standardization of bronze.erp_px_cat_g1v2 table and inserting data to silver.erp_px_cat_g1v2 table

	SET @start_time = GETDATE();
	PRINT '>> Truncating the silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data into silver.erp_px_cat_g1v2';
 
	INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
	 )
	 SELECT id,
			cat,
			subcat,
			maintenance
	  FROM bronze.erp_px_cat_g1v2
	  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
