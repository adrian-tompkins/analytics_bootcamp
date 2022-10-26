-- Databricks notebook source
use apjuice

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from apjuice.apj_sales_fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A quick peek under the hood...
-- MAGIC Lets describe the table infromation. Take note of the location.

-- COMMAND ----------

describe table extended apjuice.apj_sales_fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now lets see what files make up this table. The below command will list out and display these.

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/apjuice.db/apj_sales_fact

-- COMMAND ----------

-- MAGIC %md It's delta that's powering these tables! Lets use a delta specific command - describing the table history

-- COMMAND ----------

describe history apjuice.apj_sales_fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's enough for now - the data engieering training will dive in further into delta. Lets get back to exploring our data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Orders Master Query

-- COMMAND ----------

select
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name as store_name,
  count(*) as cnt
from
  apjuice.apj_sales_fact a
  join apjuice.dim_store_locations b on a.slocation_skey = b.slocation_skey
group by
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Trying other Languages

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df = spark.read.table('apj_sales_fact')
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val df = spark.read.table("apj_sales_fact")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC library(SparkR)
-- MAGIC library(sparklyr)
-- MAGIC library(dplyr)
-- MAGIC sc <- spark_connect(method = "databricks")
-- MAGIC fromTable <- spark_read_table(
-- MAGIC   sc   = sc,
-- MAGIC   name = "apj_sales_fact"
-- MAGIC )
-- MAGIC 
-- MAGIC collect(fromTable)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC 
-- MAGIC df_sales_fact = spark.read.table('apj_sales_fact')
-- MAGIC df_store_locations = spark.read.table('dim_store_locations')
-- MAGIC df_orders_master = (
-- MAGIC     df_sales_fact.join(
-- MAGIC         df_store_locations, 
-- MAGIC         df_sales_fact.slocation_skey == df_store_locations.slocation_skey,
-- MAGIC         "inner"
-- MAGIC     )
-- MAGIC     .groupBy(
-- MAGIC         df_sales_fact.store_id,
-- MAGIC         df_sales_fact.order_source,
-- MAGIC         df_sales_fact.order_state,
-- MAGIC         df_store_locations.city,
-- MAGIC         df_store_locations.country_code,
-- MAGIC         df_store_locations.name
-- MAGIC    )
-- MAGIC    .count()
-- MAGIC    .select(
-- MAGIC         df_sales_fact.store_id,
-- MAGIC         df_sales_fact.order_source,
-- MAGIC         df_sales_fact.order_state,
-- MAGIC         df_store_locations.city,
-- MAGIC         df_store_locations.country_code,
-- MAGIC         df_store_locations.name,
-- MAGIC         F.col("count").alias("cnt")
-- MAGIC    )
-- MAGIC )
-- MAGIC 
-- MAGIC display(df_orders_master)

-- COMMAND ----------

create or replace temp view orders_master as
select
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name as store_name,
  count(*) as cnt
from
  apjuice.apj_sales_fact a
  join apjuice.dim_store_locations b on a.slocation_skey = b.slocation_skey
group by
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df_orders_master = spark.read.table('orders_master')
-- MAGIC display(df_orders_master)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import pandas as pd
-- MAGIC df = spark.read.table('orders_master').toPandas()
-- MAGIC 
-- MAGIC df = df[(df.order_source == "ONLINE") & (df.order_state == "PENDING")]
-- MAGIC df = df.groupby(['country_code']).agg(country_cnt=('cnt', 'sum')).reset_index()
-- MAGIC 
-- MAGIC df

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import plotly.express as px
-- MAGIC fig = px.pie(df, values='country_cnt', names='country_code')
-- MAGIC fig

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import pandas as pd
-- MAGIC df = spark.read.table('orders_master').pandas_api()
-- MAGIC 
-- MAGIC df = df[(df.order_source == "ONLINE") & (df.order_state == "PENDING")]
-- MAGIC df = df.groupby(['country_code']).agg(country_cnt=('cnt', 'sum')).reset_index()
-- MAGIC 
-- MAGIC df

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import plotly.express as px
-- MAGIC fig = px.pie(df.to_pandas(), values='country_cnt', names='country_code')
-- MAGIC fig
