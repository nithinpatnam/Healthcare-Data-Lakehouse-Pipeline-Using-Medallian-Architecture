# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.gold.final_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.groupBy(col('engagement_level'))\
    .agg(
        countDistinct((col('patient_name'))).alias('total_patients'),
        count(col('visit_id')).alias('total_visits')
    )
display(updated_df)

# COMMAND ----------

updated_df.write.format('delta').saveAsTable('mactores.gold.patient_engagement_level')

# COMMAND ----------

spark.read.table('mactores.gold.patient_engagement_level').display()