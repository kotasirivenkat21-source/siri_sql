/*
COL Script: Create Silver Tables

Script Purpose:
This script creates tables in the "silver' schema, dropping existing tables
if they already exist.
Run this script to re-define the DOL structure of "bronze" Tables

....*/


use datawarehouse
go

if object_id ('silver.crm_cust_info','U') is not null
drop table silver.crm_cust_info;
create table silver.crm_cust_info (
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(10),
cst_gndr nvarchar(10),
cst_create_date date,
dwh_create_date datetime default getdate()
);

if object_id ('silver.crm_prd_info','U') is not null
drop table silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime,
dwh_create_date datetime default getdate());

--to
--SOMETIMES WE NEED TO MODIFY STRUCTURE ACCORDING TO SISTUATION
--so update this
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);




if object_id ('silver.crm_sales_details','U') is not null
drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(20),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime default getdate());

if object_id ('silver.erp_loc_a101','U') is not null
drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime default getdate());

if object_id ('silver.erp_cust_az12','U') is not null
drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50),
dwh_create_date datetime default getdate()
);

if object_id ('silver.erp_px_cat_gv12','U') is not null
drop table silver.erp_px_cat_gv12;
create table silver.erp_px_cat_gv12(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintaince nvarchar(50),
dwh_create_date datetime default getdate());


