# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.format('json').option('inferschema',True).option("header",True).option("multiline",True).load("/Volumes/mactores/input/files/patient_events.json")
display(raw_df)

# COMMAND ----------

df_final = raw_df.select(
    "*",
    col("details.department").alias("department"),
    col("details.priority").alias("priority")
).drop("details")

df_final.display()

# COMMAND ----------

# %sql
# drop table mactores.bronze.patients_raw

# COMMAND ----------

df_final.write.format('delta').saveAsTable('mactores.bronze.patients_events_raw')

# COMMAND ----------

display(spark.read.table('mactores.bronze.patients_events_raw'))

# COMMAND ----------

