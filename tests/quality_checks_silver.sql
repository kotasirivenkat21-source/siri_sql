use datawarehouse
/*

Quality Checks

Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after data loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks.

*/

-- Checking 'silver.crm_cust_info'

--

--Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results


  
--writing issues in bronze layer after updation coming here and checking data is proper or not
-- this wt first issues happened in bronze layer when loading into silver layer
--after proper cleaning
--esception :no results
--checking duplicates for the cst_id
----------------------------------------------------------------------------------------------------------------
--before check;- bronze after check:-replace bronze with silver 
--table:-crm_cust_info

select *
from(
 select *,
 row_number() over(partition by cst_id order by cst_create_date desc) s
 from silver.crm_cust_info) t
where s>1


--exception:no results
--checking for unwanted spaces
select cst_firstname from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname)


--data standadization and consistency
select distinct cst_gndr
from bronze.crm_cust_info

select * from silver.crm_cust_info

----------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------
--table:-crm_prd_info


select prd_id,
prd_key,
replace(substring(prd_key,1,5),'-','_') as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where replace(substring(prd_key,1,5),'-','_') not in 
(select distinct id from bronze.erp_px_cat_gv12)

--Check for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

---- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

select * from silver.crm_prd_info

-----------------------------------------------------------------------------------------------------------------
--silver.sales_details
--Expected null

select nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0 or len(sls_order_dt)!=8



select nullif(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt<=0 or len(sls_ship_dt)!=8
or sls_ship_dt >20500101 
or sls_ship_dt < 19000101

select nullif(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt<=0 or len(sls_due_dt)!=8
or sls_due_dt >20500101 
or sls_due_dt < 19000101

select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


select * from silver.crm_sales_details
where sls_price!=sls_quantity*sls_sales
or sls_sales is null or
sls_price is null or sls_quantity is null or sls_sales<=0 or sls_price <=0 or sls_quantity<=0


SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity,sls_price


-----------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------------

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

select 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
ELSE cid END AS cid,
case when bdate>getdate() then null else bdate end as bdate,
case when upper(trim(gen)) in ('F','FEMALE') THEN 'FEMALE'
	WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'MALE'
	ELSE 'n/a'
	end as gen
	from bronze.erp_cust_az12

select * from bronze.erp_cust_az12




-----------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------------

--Table:- erp_loc_ac101
select * from bronze.erp_loc_a101

select replace(cid,'-','') cid,
cntry from bronze.erp_loc_a101
where replace(cid,'-','') not in (
select cst_key from silver.crm_cust_info
)

select 
replace(cid,'-','') cid,
case when trim(cntry) in('USA','US','United States') 
then 'United States'
when trim(cntry) in ('DE') then 'Germany'
when trim(cntry) in ('',null) then 'n/a'
else trim(cntry) end as cntry
from bronze.erp_loc_a101

select distinct cntry from bronze.erp_loc_a101

-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--Table:-erp_px_cat_g1v2

-- Check for unwanted Spaces

SELECT * FROM bronze.erp_px_cat_gv12
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintaince != TRIM(maintaince)


-----------------------------------------------------------------------------------------------------------------
