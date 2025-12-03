/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
    DECLARE @Start_time DATETIME , @End_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
    BEGIN TRY
	PRINT '=============================';
	PRINT 'LOADING SILVER LAYER';
	PRINT '=============================';

	PRINT '-----------------------------';
	PRINT 'LOADING CRM TABLES';
	PRINT '-----------------------------';

	SET @Start_time = GETDATE();
	print '>> Truncating Table: Silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	print '>>Inserting Data info:Silver.crm_cust_info';
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
	TRIM(cst_firstname)as cst_firstname,
	TRIM(cst_lastname)as cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Marride'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date 
	FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)AS FLAG_LAST FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL)t where FLAG_LAST=1
	SET @End_time = GETDATE();
	PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
	PRINT '---------------------';

	SET @Start_time = GETDATE();
	print '>> Truncating Table: Silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	print '>>Inserting Data info:Silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
	prd_id ,
		cat_id,
		prd_key ,
		prd_nm  ,
		prd_cost ,
		prd_line ,
		prd_start_dt ,
		prd_end_dt 
	)
	select 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5), '-','_')AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END as prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt)OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE ) AS prd_end_dt
	from bronze.crm_prd_info
	SET @End_time = GETDATE();
	PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
	PRINT '---------------------';

	SET @Start_time = GETDATE();
	print '>> Truncating Table: Silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	print '>>Inserting Data info:Silver.crm_sales_details';
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
	SELECT
		sls_ord_num,
		sls_prd_key ,
		sls_cust_id  ,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt as varchar)AS DATE)
			 END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt as varchar)AS DATE)
			 END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt as varchar)AS DATE)
			 END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
			 then sls_quantity * ABS(sls_price)
			 ELSE sls_sales
			 END AS sls_sales,
		sls_quantity ,
		CASE WHEN sls_price IS NULL OR sls_price <=0
			 THEN sls_sales / NULLIF(sls_quantity,0)
			 ELSE sls_price
			 END AS sls_price
	FROM bronze.crm_sales_details
	SET @End_time = GETDATE();
	PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
	PRINT '---------------------';

	PRINT '-----------------------------';
	PRINT 'LOADING ERP TABLES';
	PRINT '-----------------------------';

	SET @Start_time = GETDATE();
	print '>> Truncating Table: Silver.erp_cust_AZ12';
	TRUNCATE TABLE silver.erp_cust_AZ12;
	print '>>Inserting Data info:Silver.erp_cust_AZ12';
	INSERT INTO silver.erp_cust_AZ12 (cid,bdate,gen)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , LEN(cid))
		 ELSE cid
	END cid ,
	CASE WHEN bdate> GETDATE() THEN NULL 
		 ELSE bdate 
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_AZ12
	SET @End_time = GETDATE();
	PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
	PRINT '---------------------';

	SET @Start_time = GETDATE();
	print '>> Truncating Table: Silver.erp_erp_LOC_A101';
	TRUNCATE TABLE silver.erp_LOC_A101;
	print '>>Inserting Data info:Silver.erp_LOC_A101';
	INSERT INTO silver.erp_LOC_A101(cid,cntry)
	SELECT 
	REPLACE(cid ,'-','') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' or cntry is null THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_LOC_A101
	SET @End_time = GETDATE();
	PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
	PRINT '---------------------';

	SET @Start_time = GETDATE();
	print '>> Truncating Table: Silver.erp_PX_CAT_G1V2';
	TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
	print '>>Inserting Data info:Silver.erp_PX_CAT_G1V2';
	INSERT INTO silver.erp_PX_CAT_G1V2(
	id,
	cat,
	subcat,
	maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_PX_CAT_G1V2
	SET @End_time = GETDATE();
	PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
	PRINT '---------------------';

	SET @batch_end_time = GETDATE();
		PRINT '===================================';
		PRINT 'LOADING SILVER LAYER IS COMPLETED'; 
		PRINT 'TOTAL LOAD DURATION:'+CAST(DATEDIFF(SECOND ,@batch_start_time , @batch_end_time )AS NVARCHAR)+'SECONDS';
	END TRY
	BEGIN CATCH
	    PRINT '============================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE'+ CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE'+ CAST (ERROR_STATE() AS NVARCHAR);
	    PRINT '============================================';
	END CATCH
END

