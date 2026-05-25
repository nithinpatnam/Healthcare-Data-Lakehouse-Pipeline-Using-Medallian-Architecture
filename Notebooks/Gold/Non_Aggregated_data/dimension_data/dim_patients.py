# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.silver.patients_data')
display(raw_df)

# COMMAND ----------

final_df=raw_df.select(
    'patient_id',
    'patient_name',
    'gender',
    'dob',
    'age'
)
display(final_df)

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.dim_patients

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.dim.dim_patients')

# COMMAND ----------

spark.read.table('mactores.dim.dim_patients').display()