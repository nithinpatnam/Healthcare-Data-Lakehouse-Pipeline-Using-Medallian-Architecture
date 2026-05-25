# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.format('csv').option('inferschema',True).option("header",True).load("/Volumes/mactores/input/files/patient_master.csv")
display(raw_df)

# COMMAND ----------

raw_df.write.format('delta').saveAsTable('mactores.bronze.patients_raw')

# COMMAND ----------

display(spark.read.table('mactores.bronze.doctors_raw'))

# COMMAND ----------

