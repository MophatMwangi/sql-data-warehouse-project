/*
Quality Checks

Script Purpose:

Usage Notes


Check 'gold.dim_customers'

Check for Uniqueness of Customer key in gold.dim_customers
--Expectation: No results
*/


SELECT prd_key,COUNT(*) FROM(
SELECT 
pn.prd_id,
pn.prd_key,
pn.prd_nm,
pn.cat_id,
pc.CAT,
pc.SUBCAT,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.MAINTENANCE
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.ID
WHERE prd_end_dt IS NULL --Filter out al historical data
) t GROUP BY prd_key
HAVING COUNT(*) > 1

SELECT cst_id, COUNT(*) FROM(SELECT 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.BDATE,
ca.GEN,
la.COUNTRY
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON         ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
ON         ci.cst_key = la.CID)t GROUP BY cst_id
HAVING COUNT(*) > 1

-- Data integration

SELECT 
DISTINCT
  ci.cst_gndr,
  ca.GEN,
 CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender Info
     ELSE COALESCE(ca.GEN,'n/a')
 END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON         ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
ON         ci.cst_key = la.CID
ORDER BY 1,2
