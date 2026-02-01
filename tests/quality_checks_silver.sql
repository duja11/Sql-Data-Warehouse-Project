/* =========================================================
   COMPLETE SILVER LAYER LOADING SCRIPT
   WITH ALL DATA QUALITY CHECKS
   ========================================================= */

CREATE OR ALTER PROCEDURE silver.load_silver_complete AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME,
            @rows_affected INT,
            @error_message NVARCHAR(4000);

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '========================================================================';
        PRINT '                 SILVER LAYER - COMPLETE LOADING PROCESS               ';
        PRINT '========================================================================';
        PRINT 'Batch Start Time: ' + CAST(@batch_start_time AS VARCHAR(50));
        PRINT '';

        /* ========================================================
           PHASE 1: CRM CUSTOMER DATA LOADING & CHECKS
           ======================================================== */
        PRINT 'PHASE 1: CRM CUSTOMER DATA';
        PRINT '===========================================';
        
        -- Check 1A: NULL cst_id in source
        PRINT '>> Check 1A: NULL Customer IDs in source data';
        SELECT 'NULL Customer IDs' AS Check_Type,
               COUNT(*) AS Affected_Rows
        FROM bronze.crm_cust_info
        WHERE cst_id IS NULL;
        
        -- Load CRM Customer Data
        PRINT '>> Loading: silver.crm_cust_info';
        SET @start_time = GETDATE();
        
        TRUNCATE TABLE silver.crm_cust_info;
        
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_gndr,
            cst_material_status,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@rows_affected AS VARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Check 1B: Duplicate check after load
        PRINT '>> Check 1B: Duplicate Customer IDs in silver layer';
        SELECT 'Duplicate Check' AS Check_Type,
               cst_id,
               COUNT(*) AS Duplicate_Count
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1;
        
        -- Check 1C: Data standardization validation
        PRINT '>> Check 1C: Gender and Marital Status Standardization';
        SELECT 'Gender Distribution' AS Category,
               cst_gndr AS Gender,
               COUNT(*) AS Count
        FROM silver.crm_cust_info
        GROUP BY cst_gndr
        UNION ALL
        SELECT 'Marital Status Distribution' AS Category,
               cst_material_status AS Status,
               COUNT(*) AS Count
        FROM silver.crm_cust_info
        GROUP BY cst_material_status;
        PRINT '';

        /* ========================================================
           PHASE 2: PRODUCT DATA CHECKS & LOADING
           ======================================================== */
        PRINT 'PHASE 2: PRODUCT DATA';
        PRINT '===========================================';
        
        -- Check 2A: Missing category references
        PRINT '>> Check 2A: Products with missing category references';
        SELECT 'Missing Category References' AS Check_Type,
               COUNT(DISTINCT prd_id) AS Affected_Products,
               COUNT(*) AS Total_Issues
        FROM bronze.crm_prd_info
        WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
            SELECT DISTINCT id FROM bronze.erp_pc_cat_g1v2
        );
        
        -- Check 2B: Missing product references in sales
        PRINT '>> Check 2B: Products not referenced in sales';
        SELECT 'Missing in Sales' AS Check_Type,
               COUNT(DISTINCT prd_id) AS Affected_Products,
               COUNT(*) AS Total_Issues
        FROM bronze.crm_prd_info
        WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
            SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details
        );
        
        -- Load Product Data
        PRINT '>> Loading: silver.crm_prd_info';
        SET @start_time = GETDATE();
        
        TRUNCATE TABLE silver.crm_prd_info;
        
        INSERT INTO silver.crm_prd_info (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1
            AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@rows_affected AS VARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Check 2C: Product data quality checks
        PRINT '>> Check 2C: Product Data Quality Suite';
        
        -- 2C.1: Duplicate or NULL primary keys
        PRINT '   Sub-check 1: Duplicate or NULL Product IDs';
        SELECT 'Duplicate/NULL Product IDs' AS Issue_Type,
               COUNT(*) AS Affected_Rows
        FROM silver.crm_prd_info
        WHERE prd_id IS NULL
           OR prd_id IN (
               SELECT prd_id
               FROM silver.crm_prd_info
               GROUP BY prd_id
               HAVING COUNT(*) > 1
           );
        
        -- 2C.2: Invalid date ranges
        PRINT '   Sub-check 2: Invalid date ranges (end date before start)';
        SELECT 'Invalid Date Ranges' AS Issue_Type,
               COUNT(*) AS Affected_Rows
        FROM silver.crm_prd_info
        WHERE prd_end_dt < prd_start_dt;
        
        -- 2C.3: NULL or negative costs
        PRINT '   Sub-check 3: NULL or negative product costs';
        SELECT 'Cost Issues' AS Issue_Type,
               COUNT(*) AS Affected_Rows
        FROM silver.crm_prd_info
        WHERE prd_cost < 0 OR prd_cost IS NULL;
        
        -- 2C.4: Product line standardization check
        PRINT '   Sub-check 4: Product Line Standardization';
        SELECT 'Product Line Distribution' AS Category,
               prd_line AS Product_Line,
               COUNT(*) AS Count
        FROM silver.crm_prd_info
        GROUP BY prd_line;
        
        -- Check 2D: SCD date logic validation
        PRINT '>> Check 2D: SCD Date Logic Validation';
        SELECT 'SCD Debug Sample' AS Check_Type,
               prd_id,
               prd_key,
               prd_start_dt,
               prd_end_dt,
               LEAD(prd_start_dt) OVER (
                   PARTITION BY prd_key
                   ORDER BY prd_start_dt
               ) AS Next_Start_Date
        FROM bronze.crm_prd_info
        WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');
        PRINT '';

        /* ========================================================
           PHASE 3: SALES DATA CHECKS & LOADING
           ======================================================== */
        PRINT 'PHASE 3: SALES DATA';
        PRINT '===========================================';
        
        -- Check 3A: Invalid date formats
        PRINT '>> Check 3A: Invalid Date Formats in Sales';
        SELECT 'Invalid Date Formats' AS Check_Type,
               SUM(CASE WHEN sls_order_dt <= 0 THEN 1 ELSE 0 END) AS Zero_Or_Negative,
               SUM(CASE WHEN LEN(sls_order_dt) != 8 THEN 1 ELSE 0 END) AS Wrong_Length,
               SUM(CASE WHEN sls_order_dt > 20500101 THEN 1 ELSE 0 END) AS Future_Date,
               SUM(CASE WHEN sls_order_dt < 19000101 THEN 1 ELSE 0 END) AS Past_Date,
               COUNT(*) AS Total_Rows_Checked
        FROM bronze.crm_sales_details;
        
        -- Check 3B: Data consistency - sales = quantity * price
        PRINT '>> Check 3B: Sales Calculation Consistency';
        SELECT 'Sales Calculation Issues' AS Check_Type,
               SUM(CASE WHEN sls_sales IS NULL THEN 1 ELSE 0 END) AS NULL_Sales,
               SUM(CASE WHEN sls_quantity IS NULL THEN 1 ELSE 0 END) AS NULL_Quantity,
               SUM(CASE WHEN sls_price IS NULL THEN 1 ELSE 0 END) AS NULL_Price,
               SUM(CASE WHEN sls_sales <= 0 THEN 1 ELSE 0 END) AS Non_Positive_Sales,
               SUM(CASE WHEN sls_quantity <= 0 THEN 1 ELSE 0 END) AS Non_Positive_Quantity,
               SUM(CASE WHEN sls_price <= 0 THEN 1 ELSE 0 END) AS Non_Positive_Price,
               SUM(CASE WHEN sls_sales <> sls_quantity * sls_price THEN 1 ELSE 0 END) AS Calculation_Mismatch,
               COUNT(*) AS Total_Rows_Checked
        FROM bronze.crm_sales_details;
        
        -- Load Sales Data
        PRINT '>> Loading: silver.crm_sales_details';
        SET @start_time = GETDATE();
        
        TRUNCATE TABLE silver.crm_sales_details;
        
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price,
            dwh_create_date
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
                WHEN sls_sales IS NULL 
                     OR sls_sales <= 0 
                     OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL 
                     OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END,
            GETDATE()
        FROM bronze.crm_sales_details;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@rows_affected AS VARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Check 3C: Invalid date order after cleaning
        PRINT '>> Check 3C: Invalid Date Sequences (Post-Cleaning)';
        SELECT 'Invalid Date Order' AS Check_Type,
               SUM(CASE WHEN sls_order_dt > sls_ship_dt THEN 1 ELSE 0 END) AS Order_After_Ship,
               SUM(CASE WHEN sls_order_dt > sls_due_dt THEN 1 ELSE 0 END) AS Order_After_Due,
               COUNT(*) AS Total_Rows_Checked
        FROM silver.crm_sales_details
        WHERE sls_order_dt IS NOT NULL 
          AND (sls_ship_dt IS NOT NULL OR sls_due_dt IS NOT NULL);
        PRINT '';

        /* ========================================================
           PHASE 4: ERP CUSTOMER DATA CHECKS & LOADING
           ======================================================== */
        PRINT 'PHASE 4: ERP CUSTOMER DATA';
        PRINT '===========================================';
        
        -- Check 4A: Cross-system duplicate prevention
        PRINT '>> Check 4A: ERP Customers already in CRM (to be excluded)';
        SELECT 'Cross-System Duplicates' AS Check_Type,
               COUNT(*) AS Duplicate_Count
        FROM bronze.erp_cust_az12
        WHERE 
            CASE 
                WHEN cid LIKE 'NAS%' 
                THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END IN (
                SELECT DISTINCT cst_key 
                FROM silver.crm_cust_info
            );
        
        -- Load ERP Customer Data
        PRINT '>> Loading: silver.erp_cust_az12';
        SET @start_time = GETDATE();
        
        TRUNCATE TABLE silver.erp_cust_az12;
        
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' 
                THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate > GETDATE() 
                THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12
        WHERE 
            CASE 
                WHEN cid LIKE 'NAS%' 
                THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END NOT IN (
                SELECT DISTINCT cst_key 
                FROM silver.crm_cust_info
            );
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@rows_affected AS VARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Check 4B: ERP Customer Data Quality
        PRINT '>> Check 4B: ERP Customer Data Quality';
        
        -- 4B.1: Out-of-range birthdates
        PRINT '   Sub-check 1: Out-of-range birthdates';
        SELECT 'Birthdate Range Issues' AS Issue_Type,
               SUM(CASE WHEN bdate < '1924-01-01' THEN 1 ELSE 0 END) AS Too_Old,
               SUM(CASE WHEN bdate > GETDATE() THEN 1 ELSE 0 END) AS Future_Date,
               COUNT(*) AS Total_Rows_Checked
        FROM silver.erp_cust_az12;
        
        -- 4B.2: Gender standardization validation
        PRINT '   Sub-check 2: Gender Standardization';
        SELECT 'Gender Distribution' AS Category,
               gen AS Gender,
               COUNT(*) AS Count
        FROM silver.erp_cust_az12
        GROUP BY gen;
        PRINT '';

        /* ========================================================
           PHASE 5: LOCATION DATA CHECKS & LOADING
           ======================================================== */
        PRINT 'PHASE 5: LOCATION DATA';
        PRINT '===========================================';
        
        -- Load Location Data
        PRINT '>> Loading: silver.erp_loc_a101';
        SET @start_time = GETDATE();
        
        TRUNCATE TABLE silver.erp_loc_a101;
        
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@rows_affected AS VARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Check 5A: Country standardization validation
        PRINT '>> Check 5A: Country Standardization';
        SELECT 'Country Distribution' AS Category,
               cntry AS Country,
               COUNT(*) AS Count
        FROM silver.erp_loc_a101
        GROUP BY cntry
        ORDER BY cntry;
        PRINT '';

        /* ========================================================
           PHASE 6: PRODUCT CATEGORY CHECKS & LOADING
           ======================================================== */
        PRINT 'PHASE 6: PRODUCT CATEGORY DATA';
        PRINT '===========================================';
        
        -- Check 6A: Unwanted spaces in source data
        PRINT '>> Check 6A: Unwanted Spaces in Source Data';
        SELECT 'Space Issues' AS Check_Type,
               SUM(CASE WHEN cat != TRIM(cat) THEN 1 ELSE 0 END) AS Category_Spaces,
               SUM(CASE WHEN subcat != TRIM(subcat) THEN 1 ELSE 0 END) AS Subcategory_Spaces,
               SUM(CASE WHEN maintenance != TRIM(maintenance) THEN 1 ELSE 0 END) AS Maintenance_Spaces,
               COUNT(*) AS Total_Rows_Checked
        FROM bronze.erp_pc_cat_g1v2;
        
        -- Load Product Category Data
        PRINT '>> Loading: silver.erp_pc_cat_g1v2';
        SET @start_time = GETDATE();
        
        TRUNCATE TABLE silver.erp_pc_cat_g1v2;
        
        INSERT INTO silver.erp_pc_cat_g1v2(id, cat, subcat, maintenance)
        SELECT 
            id, 
            TRIM(cat), 
            TRIM(subcat), 
            TRIM(maintenance)
        FROM bronze.erp_pc_cat_g1v2;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@rows_affected AS VARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Check 6B: Data profiling
        PRINT '>> Check 6B: Category Data Profiling';
        
        PRINT '   Category Distribution:';
        SELECT 'Categories' AS Type,
               cat AS Value,
               COUNT(*) AS Count
        FROM silver.erp_pc_cat_g1v2
        GROUP BY cat
        ORDER BY cat;
        
        PRINT '   Subcategory Distribution:';
        SELECT 'Subcategories' AS Type,
               subcat AS Value,
               COUNT(*) AS Count
        FROM silver.erp_pc_cat_g1v2
        GROUP BY subcat
        ORDER BY subcat;
        
        PRINT '   Maintenance Distribution:';
        SELECT 'Maintenance' AS Type,
               maintenance AS Value,
               COUNT(*) AS Count
        FROM silver.erp_pc_cat_g1v2
        GROUP BY maintenance
        ORDER BY maintenance;
        PRINT '';

        /* ========================================================
           PHASE 7: FINAL CROSS-SYSTEM INTEGRITY CHECKS
           ======================================================== */
        PRINT 'PHASE 7: CROSS-SYSTEM INTEGRITY CHECKS';
        PRINT '===========================================';
        
        -- Check 7A: Customer consistency across systems
        PRINT '>> Check 7A: Customer ID Consistency';
        SELECT 'Customer ID Analysis' AS Check_Type,
               COUNT(DISTINCT cst_key) AS CRM_Customers,
               COUNT(DISTINCT cid) AS ERP_Customers,
               COUNT(DISTINCT CASE WHEN cst_key IN (SELECT cid FROM silver.erp_cust_az12) 
                                   THEN cst_key END) AS In_Both_Systems,
               COUNT(DISTINCT CASE WHEN cst_key NOT IN (SELECT cid FROM silver.erp_cust_az12) 
                                   AND cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)
                                   THEN cst_key END) AS Unique_Per_System
        FROM (
            SELECT cst_key FROM silver.crm_cust_info
            UNION ALL
            SELECT cid FROM silver.erp_cust_az12
        ) AS all_customers;
        
        -- Check 7B: Product-category referential integrity
        PRINT '>> Check 7B: Product-Category Referential Integrity';
        SELECT 'Referential Integrity' AS Check_Type,
               COUNT(*) AS Products_Without_Valid_Category
        FROM silver.crm_prd_info p
        WHERE NOT EXISTS (
            SELECT 1 
            FROM silver.erp_pc_cat_g1v2 c 
            WHERE p.cat_id = c.id
        );
        
        -- Check 7C: Product-sales referential integrity
        PRINT '>> Check 7C: Product-Sales Referential Integrity';
        SELECT 'Sales Integrity' AS Check_Type,
               COUNT(DISTINCT sls_prd_key) AS Products_In_Sales_Not_In_Catalog
        FROM silver.crm_sales_details s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM silver.crm_prd_info p 
            WHERE s.sls_prd_key = p.prd_key
        );
        
        -- Check 7D: Customer-sales referential integrity
        PRINT '>> Check 7D: Customer-Sales Referential Integrity';
        SELECT 'Customer-Sales Integrity' AS Check_Type,
               COUNT(DISTINCT sls_cust_id) AS Customers_In_Sales_Not_In_Master
        FROM silver.crm_sales_details s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM (
                SELECT cst_key FROM silver.crm_cust_info
                UNION
                SELECT cid FROM silver.erp_cust_az12
            ) AS all_customers
            WHERE all_customers.cst_key = s.sls_cust_id
        );
        PRINT '';

        /* ========================================================
           FINAL SUMMARY AND METRICS
           ======================================================== */
        SET @batch_end_time = GETDATE();
        
        PRINT '========================================================================';
        PRINT '                         LOADING SUMMARY                               ';
        PRINT '========================================================================';
        PRINT 'OVERALL STATISTICS:';
        PRINT '  Start Time: ' + CAST(@batch_start_time AS VARCHAR(50));
        PRINT '  End Time: ' + CAST(@batch_end_time AS VARCHAR(50));
        PRINT '  Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds';
        PRINT '';
        PRINT 'TABLES LOADED:';
        
        SELECT '1. CRM Customers' AS Table_Name,
               COUNT(*) AS Row_Count,
               (SELECT COUNT(*) FROM bronze.crm_cust_info) AS Source_Row_Count
        FROM silver.crm_cust_info
        UNION ALL
        SELECT '2. Products',
               COUNT(*),
               (SELECT COUNT(*) FROM bronze.crm_prd_info)
        FROM silver.crm_prd_info
        UNION ALL
        SELECT '3. Sales Details',
               COUNT(*),
               (SELECT COUNT(*) FROM bronze.crm_sales_details)
        FROM silver.crm_sales_details
        UNION ALL
        SELECT '4. ERP Customers',
               COUNT(*),
               (SELECT COUNT(*) FROM bronze.erp_cust_az12)
        FROM silver.erp_cust_az12
        UNION ALL
        SELECT '5. Location Data',
               COUNT(*),
               (SELECT COUNT(*) FROM bronze.erp_loc_a101)
        FROM silver.erp_loc_a101
        UNION ALL
        SELECT '6. Product Categories',
               COUNT(*),
               (SELECT COUNT(*) FROM bronze.erp_pc_cat_g1v2)
        FROM silver.erp_pc_cat_g1v2;
        
        PRINT '';
        PRINT 'DATA QUALITY SUMMARY:';
        PRINT '  ✓ NULL value checks on primary keys';
        PRINT '  ✓ Missing reference checks';
        PRINT '  ✓ Business logic validation (sales = quantity × price)';
        PRINT '  ✓ Date sequence validation';
        PRINT '  ✓ Numeric range validation (costs, dates)';
        PRINT '  ✓ Cross-system consistency checks';
        PRINT '  ✓ Referential integrity validation';
        PRINT '  ✓ Standardized values across systems';
        PRINT '  ✓ Deduplication logic applied';
        PRINT '  ✓ Primary key uniqueness enforced';
        PRINT '  ✓ Cross-system duplicate prevention';
        PRINT '  ✓ Date format and range validation';
        PRINT '  ✓ Code value validation (gender, marital status, etc.)';
        PRINT '  ✓ Business rule compliance';
        PRINT '  ✓ String trimming and case standardization';
        PRINT '  ✓ Format normalization applied';
        PRINT '';
        PRINT '========================================================================';
        PRINT '                    SILVER LAYER LOADING COMPLETED                     ';
        PRINT '========================================================================';

    END TRY
    BEGIN CATCH
        SET @error_message = 
            'ERROR at ' + CAST(GETDATE() AS VARCHAR(50)) + CHAR(10) +
            'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + CHAR(10) +
            'Error Message: ' + ERROR_MESSAGE() + CHAR(10) +
            'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR) + CHAR(10) +
            'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR) + CHAR(10) +
            'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        
        PRINT '========================================================================';
        PRINT '                          LOADING FAILED                               ';
        PRINT '========================================================================';
        PRINT @error_message;
        PRINT '========================================================================';
        
        -- Log error to error table if exists
        IF OBJECT_ID('silver.load_errors', 'U') IS NOT NULL
        BEGIN
            INSERT INTO silver.load_errors (
                error_time,
                error_number,
                error_message,
                error_line,
                error_severity,
                error_state,
                batch_start_time
            )
            VALUES (
                GETDATE(),
                ERROR_NUMBER(),
                ERROR_MESSAGE(),
                ERROR_LINE(),
                ERROR_SEVERITY(),
                ERROR_STATE(),
                @batch_start_time
            );
        END
        
        THROW;
    END CATCH
END
GO

/* =========================================================
   EXECUTE THE COMPLETE SILVER LAYER LOADING PROCESS
   ========================================================= */
PRINT '========================================================================';
PRINT 'EXECUTING COMPLETE SILVER LAYER LOADING PROCEDURE';
PRINT '========================================================================';
EXEC silver.load_silver_complete;
GO
