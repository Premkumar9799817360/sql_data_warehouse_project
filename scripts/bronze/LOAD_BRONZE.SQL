-- Create the bronze layer loader procedure in Snowflake

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP();

    -- Logging start
    SYSTEM$PRINT('================================================');
    SYSTEM$PRINT('Loading Bronze Layer');
    SYSTEM$PRINT('================================================');

    -- =============================
    -- Loading CRM Tables
    -- =============================
    SYSTEM$PRINT('------------------------------------------------');
    SYSTEM$PRINT('Loading CRM Tables');
    SYSTEM$PRINT('------------------------------------------------');

    start_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Trunc bronze.crm_cust_info');
    TRUNCATE TABLE bronze.crm_cust_info;
    SYSTEM$PRINT('>> Load bronze.crm_cust_info');
    COPY INTO bronze.crm_cust_info
    FROM @bronze.crm_stage/cust_info.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    end_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Duration: ' || DATEDIFF('second', start_time, end_time) || ' seconds');

    start_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Trunc bronze.crm_prd_info');
    TRUNCATE TABLE bronze.crm_prd_info;
    SYSTEM$PRINT('>> Load bronze.crm_prd_info');
    COPY INTO bronze.crm_prd_info
    FROM @bronze.crm_stage/prd_info.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    end_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Duration: ' || DATEDIFF('second', start_time, end_time) || ' seconds');

    start_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Trunc bronze.crm_sales_details');
    TRUNCATE TABLE bronze.crm_sales_details;
    SYSTEM$PRINT('>> Load bronze.crm_sales_details');
    COPY INTO bronze.crm_sales_details
    FROM @bronze.crm_stage/sales_details.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    end_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Duration: ' || DATEDIFF('second', start_time, end_time) || ' seconds');

    -- =============================
    -- Loading ERP Tables
    -- =============================
    SYSTEM$PRINT('------------------------------------------------');
    SYSTEM$PRINT('Loading ERP Tables');
    SYSTEM$PRINT('------------------------------------------------');

    start_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Trunc bronze.erp_loc_a101');
    TRUNCATE TABLE bronze.erp_loc_a101;
    SYSTEM$PRINT('>> Load bronze.erp_loc_a101');
    COPY INTO bronze.erp_loc_a101
    FROM @bronze.erp_stage/loc_a101.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    end_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Duration: ' || DATEDIFF('second', start_time, end_time) || ' seconds');

    start_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Trunc bronze.erp_cust_az12');
    TRUNCATE TABLE bronze.erp_cust_az12;
    SYSTEM$PRINT('>> Load bronze.erp_cust_az12');
    COPY INTO bronze.erp_cust_az12
    FROM @bronze.erp_stage/cust_az12.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    end_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Duration: ' || DATEDIFF('second', start_time, end_time) || ' seconds');

    start_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Trunc bronze.erp_px_cat_g1v2');
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    SYSTEM$PRINT('>> Load bronze.erp_px_cat_g1v2');
    COPY INTO bronze.erp_px_cat_g1v2
    FROM @bronze.erp_stage/px_cat_g1v2.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    end_time := CURRENT_TIMESTAMP();
    SYSTEM$PRINT('>> Duration: ' || DATEDIFF('second', start_time, end_time) || ' seconds');

    batch_end_time := CURRENT_TIMESTAMP();

    -- Logging completion
    SYSTEM$PRINT('==========================================');
    SYSTEM$PRINT('Loading Bronze Layer Completed');
    SYSTEM$PRINT('Total Load Duration: ' || DATEDIFF('second', batch_start_time, batch_end_time) || ' seconds');
    SYSTEM$PRINT('==========================================');

    RETURN 'Bronze layer loading completed successfully.';
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$PRINT('==========================================');
        SYSTEM$PRINT('ERROR OCCURRED DURING LOADING BRONZE LAYER');
        SYSTEM$PRINT('Error Message: ' || ERROR_MESSAGE());
        SYSTEM$PRINT('==========================================');
        RETURN 'Error occurred during bronze layer loading.';
END;
$$;

-- for use and running 
USE DATABASE DATAWAREHOUSE;
USE SCHEMA BRONZE;

CALL bronze.load_bronze();


--✅ Step 1: Create the loader procedure.
snowsql -a <account> -u <user> -f create_bronze_loader_procedure.sql

--✅ Step 2: Execute the procedure to load data.
snowsql -a <account> -u <user> -f run_bronze_loader.sql





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
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\sql\dwh_project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\sql\dwh_project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\sql\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
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
