-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### AP Juice Lakehouse Platform
-- MAGIC 
-- MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceLogo.png?raw=true" style="width: 650px; max-width: 100%; height: auto" />
-- MAGIC 
-- MAGIC 
-- MAGIC For this exercise we will be creating a data model in the gold layer in Lakehouse platform for our company, AP Juice.
-- MAGIC 
-- MAGIC AP Juice has been running for a while and we already had multiple data sources that could be used. To begin with, we have decided to focus on sales transactions that are uploaded from our store locations directly to cloud storage account. In addition to sales data we already had couple of dimension tables that we have exported to files and uploaded to cloud storage as well.
-- MAGIC 
-- MAGIC For this part of the exercise we will be processing 3 existing dimensions and sales transactions datasets. Files will be a mix of `csv` and `json` files and our goal is to have modelled gold layer

-- COMMAND ----------

-- MAGIC %run ./Utils/Fetch-User-Metadata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/ANZBootcampBatch.jpg?raw=true" width=1012/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Explore what we have in the Silver Layer and the dim tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"USE {database_name};")

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from silver_sales

-- COMMAND ----------

select * from silver_sale_items

-- COMMAND ----------

select * from silver_customers

-- COMMAND ----------

select * from silver_products

-- COMMAND ----------

select * from silver_store_locations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Gold tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src='https://i.ibb.co/zQHhFcg/modelling.png'>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Customer Dimension

-- COMMAND ----------

create or replace table dim_customers
as select row_number() over (order by unique_id) as customer_skey , unique_id, store_id, name, email,'Y' as current_record,cast('1900-01-01 00:00:00'as timestamp) as start_date, cast(null as timestamp) as end_date
from silver_customers

-- COMMAND ----------

select * from dim_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Product Dimension

-- COMMAND ----------

create or replace table dim_products
as select row_number() over (order by id) as product_skey , id, ingredients, name,'Y' as current_record,cast('1900-01-01 00:00:00'as timestamp) as start_date, cast(null as timestamp) as end_date
from silver_products

-- COMMAND ----------

select * from dim_products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Store Location Dimension

-- COMMAND ----------

create or replace table dim_store_locations
as select row_number() over (order by id) as slocation_skey, id, name, city, hq_address, country_code, phone_number, 'Y' as current_record,cast('1900-01-01 00:00:00'as timestamp) as start_date, cast(null as timestamp) as end_date
from silver_store_locations

-- COMMAND ----------

select * from dim_store_locations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create APJ Sales Fact

-- COMMAND ----------

create or replace table apj_sales_fact
with apj_sales_fact_tmp as (select f.id as sale_id, f.ts, f.order_source, f.order_state, f.unique_customer_id, f.store_id
from  silver_sales f
)

select dc.customer_skey as customer_skey, dsl.slocation_skey as slocation_skey, f.* from apj_sales_fact_tmp f

/* Get the Customer SKEY record */

join dim_customers dc
on f.unique_customer_id = dc.unique_id

/* Get the Location SKEY record */
join dim_store_locations dsl 
on f.store_id = dsl.id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create APJ Sale Items Fact

-- COMMAND ----------

create or replace table apj_sale_items_fact
with apj_sale_items_fact_tmp as (select f.sale_id, f.product_id, f.store_id, f.product_size, f.product_cost, f.product_ingredients
from  silver_sale_items f
)

select dp.product_skey as product_skey, dsl.slocation_skey as slocation_skey, ss.unique_customer_id,  f.* from apj_sale_items_fact_tmp f

/* Get the Product SKEY record */

join dim_products dp
on f.product_id = dp.id

/* Get the Location SKEY record */
join dim_store_locations dsl 
on f.store_id = dsl.id

join silver_sales ss
on f.sale_id = ss.id

-- COMMAND ----------

select * from apj_sales_fact

-- COMMAND ----------

select * from apj_sale_items_fact

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC create_shared_database = True
-- MAGIC shared_database_name = "apjuice"
-- MAGIC 
-- MAGIC if create_shared_database:
-- MAGIC     spark.sql(f"DROP DATABASE IF EXISTS {shared_database_name} CASCADE")
-- MAGIC     print(shared_database_name)
-- MAGIC     spark.sql(f"CREATE DATABASE {shared_database_name}")
-- MAGIC     for table in spark.catalog.listTables():
-- MAGIC         spark.sql(f"CREATE TABLE {shared_database_name}.{table.name} AS SELECT * FROM {table.name}")
-- MAGIC     

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.listTables()
