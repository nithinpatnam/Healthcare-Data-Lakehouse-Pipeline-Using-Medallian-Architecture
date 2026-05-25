# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.silver.insurance_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.withColumn('claim_ratio',round(col('claim_ratio'),2))\
    .withColumnRenamed('claim_ratio','insurance_coverage_percent')
display(updated_df)

# COMMAND ----------

final_df=updated_df.select(
    "claim_id",
    "visit_id",
    "patient_id",
    "claim_amount",
    "claim_status",
    "insurance_coverage_percent"
)
display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists mactores.fact

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.fact_insurance_claims

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.fact.fact_insurance_claims')

# COMMAND ----------

spark.read.table('mactores.fact.fact_insurance_claims').display()