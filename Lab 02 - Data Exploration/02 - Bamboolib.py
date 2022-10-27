# Databricks notebook source
# MAGIC %md 
# MAGIC # Bamboolib
# MAGIC Bamboolib is an interactive code genration tool, that makes it easier to explore and wrangle data. You don't need to be an expert in python, pandas or plotly to get started. It also provides a great way to learn how to write code to do the analysis for you; build enough transformations in Bamboolib and you'll get the hang of it!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Bamboolib Library
# MAGIC To start using bamboolib, first we need to install the library using the below command.

# COMMAND ----------

# MAGIC %pip install bamboolib

# COMMAND ----------

# there's an underlying optimisation that was causing some problems when transitioning to toPandas()
# we'll disable this optimisation for now
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Build a Dataframe
# MAGIC We can now start bamboolib by running the below commands.
# MAGIC 
# MAGIC Use bamboolib to get the `dim_store_locations` table and load it into a dataframe called df_store_locations, with no limits on how many rows it brings back.

# COMMAND ----------

import bamboolib as bam
bam

# COMMAND ----------

# MAGIC %md
# MAGIC Paste the generate code below and run the cell:

# COMMAND ----------

# paste code to fetch the dim_store_locations here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Build the "Orders Master" Dataframe
# MAGIC 
# MAGIC Now use the Bamboolib UI to rebuild the Orders Master dataframe, `df_orders_master`, with no limits on how many rows it brings back when getting the `apj_sales_fact` table.
# MAGIC 
# MAGIC You will need to use join, group, select, and rename trasformations to do this.
# MAGIC 
# MAGIC For reference this query the we will be rebuilding
# MAGIC 
# MAGIC ```
# MAGIC select
# MAGIC   a.store_id,
# MAGIC   a.order_source,
# MAGIC   a.order_state,
# MAGIC   b.city,
# MAGIC   b.country_code,
# MAGIC   b.name as store_name,
# MAGIC   count(*) as cnt
# MAGIC from
# MAGIC   apjuice.apj_sales_fact a
# MAGIC   join apjuice.dim_store_locations b on a.slocation_skey = b.slocation_skey
# MAGIC group by
# MAGIC   a.store_id,
# MAGIC   a.order_source,
# MAGIC   a.order_state,
# MAGIC   b.city,
# MAGIC   b.country_code,
# MAGIC   b.name
# MAGIC ```

# COMMAND ----------

bam

# COMMAND ----------

# MAGIC %md
# MAGIC Paste the generate code below and run the cell:

# COMMAND ----------

# paste code to build the df_orders_master dataframe is here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Plot "Orders by Country"
# MAGIC 
# MAGIC Further transform the `df_orders_master` data frame by grouping on the country code and summing the cnt field. Then plot the result in a pie chart.

# COMMAND ----------

dim_store_locations

# COMMAND ----------

# MAGIC %md
# MAGIC Paste the code to get the country count, then the code to generate the plot in the cell below

# COMMAND ----------

# paste the transformation and plotting code here
