# Databricks notebook source
# MAGIC %md ##Install bamboolib

# COMMAND ----------

# MAGIC %pip install bamboolib

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", False)

# COMMAND ----------

# MAGIC %md ##Use bamboolib by itself

# COMMAND ----------

import bamboolib as bam
bam
