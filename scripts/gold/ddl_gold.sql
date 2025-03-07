/*
========================================================================
DDL Script: Create Gold Views
========================================================================
Script Purpose : Create Views in Gold Schema.
				 Each View performs transformations & combines the data 
				 from the silver layer to produce a clean and business 
				 ready dataset.
				 Views can be queried directly for analytical purposes
========================================================================
*/

-- ==========================================================================================
-- Creating gold.dim_customers
-- ==========================================================================================

CREATE VIEW gold.dim_customers AS
SELECT 
		ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,  --Creating a surrogate key
		ci.cst_id AS customer_id ,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		cl.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is master for gender info
			ELSE COALESCE(ca.gen,'n/a')
		END AS gender,
		ca.bdate AS birth_date,
		ci.cst_create_date AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 cl
	ON ci.cst_key = cl.cid 

-- ==========================================================================================
-- Creating gold.dim_products
-- ==========================================================================================

CREATE VIEW gold.dim_products AS
SELECT 
		ROW_NUMBER() OVER(ORDER BY prd.prd_start_dt) AS product_key,
		prd.prd_id AS product_id,
		prd.prd_key AS product_number,
		prd.prd_nm AS product_name,
		prd.cat_id AS category_id,
		cat.cat AS category,
		cat.subcat AS subcategory,
		cat.maintenance,
		prd.prd_cost AS cost,
		prd.prd_line AS product_line,
		prd.prd_start_dt AS start_date
	FROM silver.crm_prd_info prd
	LEFT JOIN silver.erp_px_cat_g1v2 cat
	ON prd.cat_id = cat.id
	WHERE prd.prd_end_dt IS NULL  --Filtering out historical data



-- ==========================================================================================
-- Creating gold.fact_sales
-- ==========================================================================================
CREATE VIEW gold.fact_sales AS 
SELECT
		sd.sls_ord_num	AS order_number,
		dp.product_key,
		dc.customer_key,
		sd.sls_order_dt AS order_date,
		sd.sls_ship_dt AS shipping_date,
		sd.sls_due_dt AS due_date,
		sd.sls_sales AS sales_amount,
		sd.sls_quantity AS quantity,
		sd.sls_price AS price
	FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products dp
	on sd.sls_prd_key = dp.product_number
	LEFT JOIN gold.dim_customers dc
	ON sd.sls_cust_id = dc.customer_id

	SELECT * FROM gold.fact_sales

	SELECT * FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c.customer_key = f.customer_key
