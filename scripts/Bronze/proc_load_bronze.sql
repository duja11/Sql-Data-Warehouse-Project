/* =========================================================
   Stored Procedure: bronze.load_bronze
   Description:
   Procédure d’ingestion de la couche Bronze.
   Elle charge les données brutes depuis des fichiers CSV
   (CRM & ERP) vers les tables Bronze via BULK INSERT.
   - Stratégie : TRUNCATE + RELOAD
   - Aucun traitement métier (raw data)
   - Logs de performance intégrés
   ========================================================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    /* Variables de suivi du temps */
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY

        /* Début du batch global */
        SET @batch_start_time = GETDATE();

        PRINT '=========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=========================================';

        /* =================================================
           Chargement des tables CRM
           ================================================= */
        PRINT '-----------------------------------------';
        PRINT 'Loading CRM tables';
        PRINT '-----------------------------------------';

        /* ----------- CRM : Customer Info ----------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_cust_info';

        -- Suppression complète des données existantes (Bronze = overwrite)
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting table: bronze.crm_cust_info';

        -- Chargement brut depuis fichier CSV
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\intel\Desktop\sources\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,            -- Ignore l’en-tête CSV
            FIELDTERMINATOR = ',',   -- Séparateur de colonnes
            TABLOCK                  -- Optimisation performance
        );

        SET @end_time = GETDATE();
        PRINT '>> load duration: ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';

        PRINT '------------------------------------';

        /* ----------- CRM : Product Info ----------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_prd_info';

        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting table: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\intel\Desktop\sources\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> load duration: ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';

        PRINT '------------------------------------';

        /* ----------- CRM : Sales Details ----------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_sales_details';

        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting table: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\intel\Desktop\sources\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> load duration: ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';

        /* =================================================
           Chargement des tables ERP
           ================================================= */
        PRINT '-----------------------------------------';
        PRINT 'Loading ERP tables';
        PRINT '-----------------------------------------';

        /* ----------- ERP : Customer Demographics ----------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_cust_az12';

        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting table: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\intel\Desktop\sources\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> load duration: ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';

        PRINT '------------------------------------';

        /* ----------- ERP : Location ----------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_loc_a101';

        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting table: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\intel\Desktop\sources\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> load duration: ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';

        PRINT '------------------------------------';

        /* ----------- ERP : Product Categories ----------- */
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_pc_cat_g1v2';

        TRUNCATE TABLE bronze.erp_pc_cat_g1v2;

        PRINT '>> Inserting table: bronze.erp_pc_cat_g1v2';
        BULK INSERT bronze.erp_pc_cat_g1v2
        FROM 'C:\Users\intel\Desktop\sources\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        PRINT '>> load duration: ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';

        /* Fin du batch */
        SET @batch_end_time = GETDATE();

        PRINT '==============================';
        PRINT 'Loading bronze layer is completed';
        PRINT '   - Total load duration: ' 
              + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '==============================';

    END TRY
    BEGIN CATCH
        /* Gestion centralisée des erreurs */
        PRINT '❌ Error while loading Bronze layer';
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    END CATCH
END;
GO
