# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.format('csv').option('header',True).option("inferschema",True).load("/Volumes/mactores/input/files/insurance_claims.csv")
display(raw_df)

# COMMAND ----------

raw_df.write.format("delta").saveAsTable('mactores.bronze.insurance_claims_raw')

# COMMAND ----------

display('mactores.bronze.insurance_claims_raw')

# COMMAND ----------

