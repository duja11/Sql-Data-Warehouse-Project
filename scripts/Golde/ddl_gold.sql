/* =========================================================
   GOLD LAYER – DIMENSION CUSTOMERS
   Purpose:
   Build the Customer Dimension for the Sales Data Mart.
   This view integrates CRM and ERP data into a unified
   analytical structure.
   ========================================================= */

CREATE VIEW gold.dim_customers AS

SELECT 
    -- Surrogate key generated for analytical joins
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

    -- Business identifiers from CRM
    ci.cst_id  AS customer_id,
    ci.cst_key AS customer_number,

    -- Descriptive attributes
    ci.cst_firstname AS firstname,
    ci.cst_lastname  AS lastname,

    -- NOTE: Column naming inconsistency (cst_gndr mapped as marital_status)
    -- This should be reviewed for semantic correctness
    ci.cst_gndr AS marital_status,

    -- Gender resolution logic:
    -- CRM is considered the master source.
    -- If marital status is not 'n/a', keep CRM value.
    -- Otherwise fallback to ERP gender.
    CASE 
        WHEN ci.cst_material_status <> 'n/a' 
        THEN ci.cst_material_status
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    -- Metadata attributes
    ci.cst_create_date AS create_date,

    -- ERP enrichment
    ca.bdate AS birthdate,
    la.cntry AS country

FROM silver.crm_cust_info ci

-- Enrich with ERP customer demographics
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

-- Enrich with customer location
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;



/* =========================================================
   GOLD LAYER – DIMENSION PRODUCTS
   Purpose:
   Build Product Dimension.
   Only active (current) product versions are included.
   Historical records (SCD Type 2) are filtered out.
   ========================================================= */

CREATE VIEW gold.dim_products AS

SELECT 
    -- Surrogate key for analytical joins
    ROW_NUMBER() OVER (
        ORDER BY pn.prd_start_dt, pn.prd_key
    ) AS product_key,

    -- Business identifiers
    pn.prd_id  AS product_id,
    pn.prd_key AS product_number,

    -- Descriptive attributes
    pn.prd_nm  AS product_name,
    pn.cat_id  AS category_id,

    -- Category enrichment
    pc.cat     AS category,
    pc.subcat  AS subcategory,
    pc.maintenance,

    -- Product metrics
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,

    -- SCD start date
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn

-- Join category reference table
LEFT JOIN silver.erp_pc_cat_g1v2 pc
    ON pn.cat_id = pc.id

-- Keep only current active records (SCD Type 2 logic)
WHERE prd_end_dt IS NULL;



/* =========================================================
   GOLD LAYER – FACT SALES
   Purpose:
   Central fact table linking customers and products.
   Contains measurable business events.
   ========================================================= */

CREATE VIEW gold.dim_fact_sales AS

SELECT 
    -- Business transaction identifier
    sd.sls_ord_num AS order_number,

    -- Foreign keys to dimensions
    pr.product_key,
    cu.customer_key,

    -- Original business keys (kept for traceability)
    sd.sls_prd_key,
    sd.sls_cust_id,

    -- Dates
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

    -- Measures
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price

FROM silver.crm_sales_details sd

-- Join Product Dimension
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number

-- Join Customer Dimension
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;



/* =========================================================
   FOREIGN KEY INTEGRITY CHECK
   Purpose:
   Validate that all fact records have matching dimension keys.
   Expectation: No rows returned.
   ========================================================= */

SELECT *
FROM gold.dim_fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL;
