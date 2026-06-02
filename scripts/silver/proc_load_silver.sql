/*
Actions Performed:
- Truncates Silver tables.

Stored Procedure: Load Silver Layer (Bronze -> Silver)

Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to
populate the 'silver' schema tables from the 'bronze' schema.

- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
*/

USE datawarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver_layer
AS
BEGIN
SET NOCOUNT ON;

------------------------------------------------------------------------------------
-- Batch Variables
DECLARE @BatchStartTime DATETIME = GETDATE();
DECLARE @BatchEndTime DATETIME;

DECLARE @StartTime DATETIME;
DECLARE @EndTime DATETIME;

PRINT '===========================================';
PRINT '>> SILVER LAYER LOAD STARTED';
PRINT '>> BATCH START TIME: ' + CAST(@BatchStartTime AS VARCHAR);
PRINT '===========================================';

BEGIN TRY

------------------------------------------------------------------------------------
-- Table 1: crm_cust_info
SET @StartTime = GETDATE();
PRINT '>> Loading: silver.crm_cust_info';

TRUNCATE TABLE silver.crm_cust_info;

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
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'single'
        WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'married'
        ELSE 'N/A' 
    END,
    CASE 
        WHEN UPPER(cst_gndr)='F' THEN 'FEMALE'
        WHEN UPPER(cst_gndr)='M' THEN 'MALE'
        ELSE 'N/A' 
    END,
    cst_create_date
FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) s
    FROM bronze.crm_cust_info
) t
WHERE s=1;

SET @EndTime = GETDATE();
PRINT '>> Time Taken (crm_cust_info): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR) + ' sec';

------------------------------------------------------------------------------------
-- Table 2: crm_prd_info
SET @StartTime = GETDATE();
PRINT '>> Loading: silver.crm_prd_info';

TRUNCATE TABLE silver.crm_prd_info;

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
    REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
    SUBSTRING(prd_key,7,LEN(prd_key)),
    prd_nm,
    ISNULL(prd_cost,0),
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    CAST(prd_start_dt AS DATE),
    CAST(
        DATEADD(DAY, -1, 
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key 
                ORDER BY prd_start_dt
            )
        ) 
    AS DATE)
FROM bronze.crm_prd_info;

SET @EndTime = GETDATE();
PRINT '>> Time Taken (crm_prd_info): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR) + ' sec';

------------------------------------------------------------------------------------
-- Table 3: crm_sales_details
SET @StartTime = GETDATE();
PRINT '>> Loading: silver.crm_sales_details';

TRUNCATE TABLE silver.crm_sales_details;

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
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END
FROM bronze.crm_sales_details;

SET @EndTime = GETDATE();
PRINT '>> Time Taken (crm_sales_details): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR) + ' sec';

------------------------------------------------------------------------------------
-- Table 4: erp_cust_az12
SET @StartTime = GETDATE();
PRINT '>> Loading: silver.erp_cust_az12';

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12(
    cid,
    bdate,
    gen
)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid 
    END,
    CASE 
        WHEN bdate > GETDATE() THEN NULL 
        ELSE bdate 
    END,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'FEMALE'
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'MALE'
        ELSE 'n/a'
    END
FROM bronze.erp_cust_az12;

SET @EndTime = GETDATE();
PRINT '>> Time Taken (erp_cust_az12): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR) + ' sec';

------------------------------------------------------------------------------------
-- Table 5: erp_loc_a101
SET @StartTime = GETDATE();
PRINT '>> Loading: silver.erp_loc_a101';

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101(
    cid,
    cntry
)
SELECT 
    REPLACE(cid,'-',''),
    CASE 
        WHEN TRIM(cntry) IN ('USA','US','United States') THEN 'United States'
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
        ELSE TRIM(cntry)
    END
FROM bronze.erp_loc_a101;

SET @EndTime = GETDATE();
PRINT '>> Time Taken (erp_loc_a101): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR) + ' sec';

------------------------------------------------------------------------------------
-- Table 6: erp_px_cat_gv12
SET @StartTime = GETDATE();
PRINT '>> Loading: silver.erp_px_cat_gv12';

TRUNCATE TABLE silver.erp_px_cat_gv12;

INSERT INTO silver.erp_px_cat_gv12(
    id,
    cat,
    subcat,
    maintaince
)
SELECT 
    id,
    cat,
    subcat,
    maintaince
FROM bronze.erp_px_cat_gv12;

SET @EndTime = GETDATE();
PRINT '>> Time Taken (erp_px_cat_gv12): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR) + ' sec';

------------------------------------------------------------------------------------
-- Batch End
SET @BatchEndTime = GETDATE();

PRINT '===========================================';
PRINT '>> BATCH END TIME: ' + CAST(@BatchEndTime AS VARCHAR);
PRINT '>> TOTAL TIME: ' + CAST(DATEDIFF(SECOND,@BatchStartTime,@BatchEndTime) AS VARCHAR) + ' sec';
PRINT '>> SILVER LOAD SUCCESS';
PRINT '===========================================';

END TRY

BEGIN CATCH
    PRINT '===========================================';
    PRINT '>> ERROR OCCURRED';
    PRINT ERROR_MESSAGE();
    PRINT '===========================================';
END CATCH

END;
GO

exec silver.load_silver_layer;
go
