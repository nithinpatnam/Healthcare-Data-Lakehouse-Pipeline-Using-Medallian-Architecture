# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.silver.visits_data')
display(raw_df)

# COMMAND ----------

final_df=raw_df.select(
    "visit_id",
    "patient_id",
    "doctor_id",
    "hospital_id",
    "visit_date",
    "discharge_date",
    "length_of_stay",
    "diagnosis",
    "total_bill"
)
display(final_df)

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.fact_patient_visits

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.fact.fact_patient_visits')

# COMMAND ----------

spark.read.table('mactores.fact.fact_patient_visits').display()