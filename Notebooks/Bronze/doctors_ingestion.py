# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

storage_account= 'databricksampleextdl'
container = 'mactores'

# COMMAND ----------

raw_df=spark.read.format('csv').option('header',True).option("inferschema",True).load("/Volumes/mactores/input/files/doctor_master.csv")
display(raw_df)

# COMMAND ----------

raw_df.write.format('delta').saveAsTable("mactores.bronze.doctors_raw")

# COMMAND ----------

spark.read.table('mactores.bronze.doctors_raw').display()

# COMMAND ----------

# raw_df.write.format("parquet") \
#     .mode("overwrite") \
#     .save(f"abfss://{container}@{storage_account}.dfs.core.windows.net/output/bronze/doctors_raw")

# COMMAND ----------

# display(spark.read.parquet(f"abfss://{container}@{storage_account}.dfs.core.windows.net/output/bronze/doctors_raw"))