# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.silver.hospital_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.withColumnRenamed('city','hospital_city')
display(updated_df)

# COMMAND ----------

final_df=updated_df.select(
    "hospital_id",
    'hospital_name',
    "hospital_city",
    "hospital_size"
)
display(final_df)

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.dim_hospitals

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.dim.dim_hospitals')

# COMMAND ----------

spark.read.table('mactores.dim.dim_hospitals').display()