# Databricks notebook source
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution to Task 1

# COMMAND ----------

import pandas as pd; import numpy as np
df_store_locations = spark.table("spark_catalog.apjuice.dim_store_locations").toPandas()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution to Task 2

# COMMAND ----------

import pandas as pd; import numpy as np
df = spark.table("spark_catalog.apjuice.apj_sales_fact").toPandas()
# Step: Inner Join with df_store_locations where slocation_skey=slocation_skey
df_orders_master = pd.merge(df, df_store_locations, how='inner', on=['slocation_skey'])

# Step: Group by and aggregate
df_orders_master = df_orders_master.groupby(['store_id', 'order_source', 'order_state', 'city', 'country_code', 'name']).agg({col: ['size'] for col in df_orders_master.columns})
df_orders_master.columns = ['_'.join(multi_index) for multi_index in df_orders_master.columns.ravel()]
df_orders_master = df_orders_master.reset_index()

# Step: Select columns
df_orders_master = df_orders_master[['store_id', 'order_source', 'order_state', 'city', 'country_code', 'name', 'store_id_size']]

# Step: Rename multiple columns
df_orders_master = df_orders_master.rename(columns={'name': 'store_name', 'store_id_size': 'cnt'})

df_orders_master

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution to Task 3

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Group by country_code and calculate new column(s)
df_country_count = df_orders_master.groupby(['country_code']).agg(cnt=('cnt', 'sum')).reset_index()

import plotly.express as px
fig = px.pie(df_country_count, values='cnt', names='country_code')
fig
