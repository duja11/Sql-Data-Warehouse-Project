/* =========================================================
   Table: bronze.crm_cust_info
   Source: CRM
   Description:
   Table brute contenant les informations clients telles
   qu’extraites du système CRM, sans transformation.
   ========================================================= */
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,                    -- Identifiant interne du client
    cst_key NVARCHAR(50),           -- Clé métier unique du client (CRM)
    cst_firstname NVARCHAR(50),     -- Prénom du client
    cst_lastname NVARCHAR(50),      -- Nom du client
    cst_material_status NVARCHAR(50), -- Statut matrimonial du client
    cst_gndr NVARCHAR(50),          -- Genre du client
    cst_create_date DATE            -- Date de création du client dans le CRM
);


/* =========================================================
   Table: bronze.crm_prd_info
   Source: CRM
   Description:
   Données produits brutes issues du CRM.
   Historique de validité géré via dates de début et fin.
   ========================================================= */
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,                     -- Identifiant interne du produit
    prd_key NVARCHAR(50),            -- Clé métier du produit
    prd_nm NVARCHAR(50),             -- Nom du produit
    prd_cost INT,                    -- Coût du produit
    prd_line NVARCHAR(50),           -- Ligne / famille de produit
    prd_start_dt DATETIME,           -- Date de début de validité
    prd_end_dt DATETIME              -- Date de fin de validité
);


/* =========================================================
   Table: bronze.crm_sales_details
   Source: CRM
   Description:
   Données transactionnelles de ventes.
   Toutes les dates sont stockées sous forme brute (INT),
   telles qu’extraites du système source.
   ========================================================= */
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),        -- Numéro de commande
    sls_prd_key NVARCHAR(50),         -- Clé produit vendue
    sls_cust_id INT,                 -- Identifiant client
    sls_order_dt INT,                -- Date de commande (format source)
    sls_ship_dt INT,                 -- Date d’expédition (format source)
    sls_due_dt INT,                  -- Date de livraison prévue (format source)
    sls_sales INT,                   -- Montant total des ventes
    sls_quantity INT,                -- Quantité vendue
    sls_price INT                   -- Prix unitaire
);


/* =========================================================
   Table: bronze.erp_loc_a101
   Source: ERP
   Description:
   Table de référence brute des localisations clients.
   ========================================================= */
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),                -- Identifiant client (ERP)
    cntry NVARCHAR(50)               -- Pays du client
);


/* =========================================================
   Table: bronze.erp_cust_az12
   Source: ERP
   Description:
   Informations démographiques clients issues de l’ERP.
   ========================================================= */
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),                -- Identifiant client (ERP)
    bdate DATE,                      -- Date de naissance
    gen NVARCHAR(50)                 -- Genre
);


/* =========================================================
   Table: bronze.erp_pc_cat_g1v2
   Source: ERP
   Description:
   Référentiel produit : catégories et sous-catégories.
   ========================================================= */
CREATE TABLE bronze.erp_pc_cat_g1v2 (
    id NVARCHAR(50),                 -- Identifiant produit
    cat NVARCHAR(50),                -- Catégorie principale
    subcat NVARCHAR(50),             -- Sous-catégorie
    maintenance NVARCHAR(50)         -- Indicateur / type de maintenance
);
