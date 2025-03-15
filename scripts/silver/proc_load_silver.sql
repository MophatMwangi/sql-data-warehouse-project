
/*
============================================================================================
Stored Procedure: Load Silver Layer (Source -> Silver)
=============================================================================================
Script Purpose:
This stored procedure performs the  ETL (Extractm, Transform, Load) process to populate the 
'silver' schema tables from the 'bronze' schema.
Actions Performed:
-Truncates Silver tables .
-Inserts transformed and cleansed data from Bronze into Silver tables

Parameters:
None.
This stored procedure does not accept any parameters or return any values.
Usage Example:
EXEC silver.load_silver
============================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
   DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY 
    SET @batch_start_time  = GETDATE()

	    PRINT '===============================';
		PRINT 'Loading Silver Layer'
		PRINT '===============================';
	
		PRINT '-------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------';


		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>Inserting Table:  silver.crm_sales_details';

		-- Loading
		SET @start_time = GETDATE();
		INSERT INTO silver.crm_sales_details(
			 sls_ord_num,
			 sls_prd_key,
			 sls_cust_id ,
			 sls_order_dt,	
			 sls_ship_dt,
			 sls_due_dt	,
			 sls_sales,
			 sls_quantity,
			 sls_price
		  )
		  SELECT 
			   sls_ord_num,
			   sls_prd_key,
			   sls_cust_id,
   			   CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE  CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			   END AS sls_order_date,
			   sls_ship_dt,
			   sls_due_dt,
			   CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END AS sls_sales,
			   sls_quantity,
			   CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
				END AS sls_price
				FROM bronze.crm_sales_details;
        
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:silver.crm_sales_details' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';
		--loading
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>Inserting Table:  silver.crm_prd_info';
		SET @start_time = GETDATE();
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
		 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, --Extract category ID
		 SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,    --Extract product key
		 prd_nm,
		 ISNULL(prd_cost,0) AS prd_cost,
		 CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN  'R' THEN 'Road'
				WHEN  'S' THEN 'other Sales'
				WHEN  'T' THEN 'Touring'
			 ELSE 'n/a'
		END prd_line, --Map product line codes to descriptive values
		 prd_start_dt,
		 DATEADD(DAY,-1,LEAD(prd_start_dt,1,NULL) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )) AS prd_end_dt_test --Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: silver.crm_prd_info' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';
		--loading
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>Inserting Table:  silver.crm_sales_details';
		SET @start_time = GETDATE();
		INSERT INTO silver.crm_sales_details(
			 sls_ord_num,
			 sls_prd_key,
			 sls_cust_id ,
			 sls_order_dt,	
			 sls_ship_dt,
			 sls_due_dt	,
			 sls_sales,
			 sls_quantity,
			 sls_price
		  )
		  SELECT 
			   sls_ord_num,
			   sls_prd_key,
			   sls_cust_id,
   			   CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE  CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			   END AS sls_order_date,
			   sls_ship_dt,
			   sls_due_dt,
			   CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END AS sls_sales,
			   sls_quantity,
			   CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
				END AS sls_price
		  FROM bronze.crm_sales_details;

	    SET @end_time = GETDATE();
		PRINT '>> Load Duration: silver.crm_sales_details' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';
		-- loading
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>Inserting Table: silver.erp_cust_az12';
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) --Remove 'NAS' prefix if present
		ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
		END bdate, -- Set future birthdates to NULL
		CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
			 ELSE 'n/a'
		END AS gen --Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: silver.erp_cust_az12' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';

		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>Inserting Table:  silver.erp_loc_a101';
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_loc_a101(cid,country)
		SELECT 
		REPLACE(CID,'-','') CID,
		CASE WHEN TRIM(COUNTRY) = 'DE'THEN 'Germany'
			 WHEN TRIM(COUNTRY) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(COUNTRY) = '' OR COUNTRY IS NULL THEN 'n/a'
			 ELSE TRIM(COUNTRY)
		END AS COUNTRY
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: silver.erp_loc_a101' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';

		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>Inserting Table:  silver.erp_px_cat_g1v2'
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: silver.erp_px_cat_g1v2' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------';
		SET @batch_end_time  = GETDATE();
		PRINT '===============================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '>> Load Duration: silver.erp_px_cat_g1v2  ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
		PRINT '===============================';

		END TRY
	BEGIN CATCH
	    PRINT '===============================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		 PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===============================';
	END CATCH
END
