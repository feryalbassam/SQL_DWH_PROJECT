/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @Start_time DATETIME , @End_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
    BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=============================';

		PRINT '-----------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '-----------------------------';

		SET @Start_time = GETDATE();
		PRINT'#TRUNCATING THE TABLE bronze.crm_cust_info ';
		truncate table bronze.crm_cust_info;

		PRINT'#INSERTING THE DATA INTO bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\adamb\Desktop\sql project\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 

		) ;
		SET @End_time = GETDATE();
		PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
		PRINT '---------------------';

		SET @Start_time = GETDATE();
		PRINT'#TRUNCATING THE TABLE bronze.crm_prd_info ';
		truncate table bronze.crm_prd_info;

		PRINT'#INSERTING THE DATA INTO bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\adamb\Desktop\sql project\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 

		) ;
		SET @End_time = GETDATE();
		PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
		PRINT '---------------------';

		PRINT'#TRUNCATING THE TABLE bronze.crm_sales_details ';
		SET @Start_time = GETDATE();
		truncate table bronze.crm_sales_details;

		PRINT'#INSERTING THE DATA INTO bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\adamb\Desktop\sql project\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 

		) ;
		SET @End_time = GETDATE();
		PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
		PRINT '---------------------';

		PRINT '-----------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '-----------------------------';

		SET @Start_time = GETDATE();
		PRINT'#TRUNCATING THE TABLE bronze.erp_cust_az12 ';
		truncate table bronze.erp_CUST_AZ12;

		PRINT'#INSERTING THE DATA INTO bronze.erp_CUST_AZ12 ';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\adamb\Desktop\sql project\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 

		) ;
		SET @End_time = GETDATE();
		PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
		PRINT '---------------------';

		SET @Start_time = GETDATE();
		PRINT'#TRUNCATING THE TABLE bronze.erp_loc_a101 ';
		truncate table bronze.erp_LOC_A101;

		PRINT'#INSERTING THE DATA INTO bronze.erp_loc_A101 ';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\adamb\Desktop\sql project\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 

		) ;
		SET @End_time = GETDATE();
		PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
		PRINT '---------------------';

		SET @Start_time = GETDATE();
		PRINT'#TRUNCATING THE TABLE bronze.erp_px_cat_g1v2 ';
		truncate table bronze.erp_PX_CAT_G1V2;

		PRINT'#INSERTING THE DATA INTO bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\adamb\Desktop\sql project\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 

		) ;
		SET @End_time = GETDATE();
		PRINT'#LOAD DURATION:'+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+'seconds';
		PRINT '---------------------';

		SET @batch_end_time = GETDATE();
		PRINT '===================================';
		PRINT 'LOADING BRONZE LAYER IS COMPLETED'; 
		PRINT 'TOTAL LOAD DURATION:'+CAST(DATEDIFF(SECOND ,@batch_start_time , @batch_end_time )AS NVARCHAR)+'SECONDS';
	END TRY
	BEGIN CATCH
	    PRINT '============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZER LAYER';
		PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE'+ CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE'+ CAST (ERROR_STATE() AS NVARCHAR);
	    PRINT '============================================';
	END CATCH
END
