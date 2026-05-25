# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.silver.doctors_data')
display(raw_df)

# COMMAND ----------

final_df=raw_df.select(
    'doctor_id',
    'doctor_name',
    'specialization',
    'experience_level'
)
display(final_df)

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.dim_doctors

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.dim.dim_doctors')

# COMMAND ----------

spark.read.table('mactores.dim.dim_doctors').display()

# COMMAND ----------

