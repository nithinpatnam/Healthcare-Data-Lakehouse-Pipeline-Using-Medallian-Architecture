# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.format('csv').option('header',True).option("inferschema",True).load("/Volumes/mactores/input/files/hospital_master.csv")
display(raw_df)

# COMMAND ----------

raw_df.write.format("delta").saveAsTable("mactores.bronze.hospital_raw")

# COMMAND ----------

display(spark.read.table('mactores.bronze.hospital_raw'))

# COMMAND ----------

