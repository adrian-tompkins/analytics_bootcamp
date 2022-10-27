-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Exploration with Notebooks
-- MAGIC 
-- MAGIC <img src="https://github.com/zivilenorkunaite/apjbootcamp2022/blob/main/images/APJuiceLogo.png?raw=true" width="400px" />
-- MAGIC 
-- MAGIC We've seen how to build dashboards and queries with Databricks SQL, now lets take a look at how we can explore that same data using notebooks.
-- MAGIC 
-- MAGIC Notebooks are provide very **powerful and flexible** way to explore data, and let your **data tell a story**. To help readers undrestand your story, you can use markdown and images for formatting. Double click on this cell to see how it works!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First let's take show the tables in our **apjuice** database:

-- COMMAND ----------

use apjuice;
show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now lets query our table using SQL

-- COMMAND ----------

select * from apjuice.apj_sales_fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A quick peek under the hood...
-- MAGIC Lets describe the table infromation. Take note of the **Location** and **Provider** properties.

-- COMMAND ----------

describe table extended apjuice.apj_sales_fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now lets see what files make up this table. Copy and paste the location in the command below to list and display the files.

-- COMMAND ----------

-- MAGIC %fs ls <paste location here>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <img src="https://www.databricks.com/wp-content/uploads/2019/09/delta-lake-logo.png" width="100px" />
-- MAGIC See the <b>_delta_log</b> folder, and the <b>delta</b> provider property? It's delta that's powering these tables! Lets use a delta specific command - describing the table history

-- COMMAND ----------

describe history apjuice.apj_sales_fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There's not much in the history for this table, but as the table changes and updates are made, the history will grow.
-- MAGIC 
-- MAGIC That's enough for now - the data engieering training will dive in further into delta. Lets get back to exploring our data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Trying other Languages
-- MAGIC 
-- MAGIC SQL is not the only language that we can use - Databricks also support python, scala and R.
-- MAGIC 
-- MAGIC Note at the top of the this notebook, the default langage is SQL. This means if we want to use other languages we will have to use **magic** commands - eg `%python` `%scala` or `%r`
-- MAGIC 
-- MAGIC Lets take a look at reading the `apj_sales_fact` table in python:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df = spark.read.table('apj_sales_fact')
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's try it in Scala

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val df = spark.read.table("apj_sales_fact")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finally, lets move to R

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

-- MAGIC %md
-- MAGIC For the rest of this notebook, we will use two of the most popular langages in Databricks - python and SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Orders Master Query
-- MAGIC 
-- MAGIC Lets re-produce the **Orders Masters Query** that we made in the SQL lab.
-- MAGIC 
-- MAGIC Try and re-create the **Orders by Country** pi-chart using the in-built visualisation tool.

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
-- MAGIC 
-- MAGIC Now lets take a look at how you would write that same query with pyspark:

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

-- MAGIC %md
-- MAGIC Depending on your preference, you might find the python code harder to read than SQL! Lets create a temporary view to make it easier to use this query in python.

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

-- MAGIC %md
-- MAGIC Now we can query this temporary view likes it's a table! Lets try it out:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df_orders_master = spark.read.table('orders_master')
-- MAGIC display(df_orders_master)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Moving to Pandas
-- MAGIC 
-- MAGIC Pandas is a very popular libarary for data manipulation and exploration. This library is fully supported in Databricks! Lets take our temporary view, filter it, and aggregate by country code

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

-- MAGIC %md
-- MAGIC #### Plotting
-- MAGIC Above, we used the in-built Databricks visualisation tools to plot the data. However - this comes with a drawback when using version control. Notice how this notebook is connected to a repo? Press the button that says _main_ right near the top of the notebook - you should see all the changes that have bene made. However, notice that it doesn't capture the pie-chart visualisation? This is becasue we need to codify the visualisation. Lets do this using the plotly library:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import plotly.express as px
-- MAGIC fig = px.pie(df, values='country_cnt', names='country_code')
-- MAGIC fig

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Now this plot is completly reproducible and saved in version control! 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Distributed Pandas
-- MAGIC Spark and Databricks is a *distributed* computing platform, meaning that we can process huge volumes of data by running it across multiple machines. However, the default pandas library, doesn't support this. When we ran the `toPandas()` call, we moved all of that data onto one of the machines in the cluster.
-- MAGIC 
-- MAGIC However, we now have the ability to run panadas code in a distributed way - called **pandas on spark**. Simply use the `pandas_api()` call instead of `toPandas()` and you'll be working with distributed pandas! You now have the ability to work with multi-billion row tables!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df = spark.read.table('orders_master').pandas_api()
-- MAGIC 
-- MAGIC df = df[(df.order_source == "ONLINE") & (df.order_state == "PENDING")]
-- MAGIC df = df.groupby(['country_code']).agg(country_cnt=('cnt', 'sum')).reset_index()
-- MAGIC 
-- MAGIC df

-- COMMAND ----------

-- MAGIC %md
-- MAGIC However, there are some limitations when it comes to pandas on spark. For example, you can't use the plotly library. Youll need to convert the pandas on spark dataframe to a regular pandas dataframe with the `to_pandas()` call before using it

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import plotly.express as px
-- MAGIC fig = px.pie(df.to_pandas(), values='country_cnt', names='country_code')
-- MAGIC fig

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Making Python and Pandas Easier
-- MAGIC Using python, pandas and plotly is a great and powerful way to interact with data, especially for users who proficient at all three! But what if you are not so comfortable with ethier coding in python, or not familiar with the pandas or plotly apis?
-- MAGIC 
-- MAGIC Move on to the next notebook `02 - Bamboolib` to see how Bamboolib can help.
