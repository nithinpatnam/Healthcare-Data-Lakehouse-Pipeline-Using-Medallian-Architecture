# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.silver.patient_events_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.groupBy(col('patient_id')).agg(count('event_id').alias('number_of_events'))\
    .withColumn('engagement_level',
        when(col('number_of_events')>10,'High')\
        .when((col('number_of_events')<=10) & (col('number_of_events')>=5),'Medium')\
        .when(col('number_of_events')<5,'Low')\
        .otherwise('Unknown'))\
    .withColumnRenamed('number_of_events','total_events')
display(updated_df)

# COMMAND ----------

final_df=updated_df.select(
    "patient_id",
    "total_events",
    "engagement_level"
)
display(final_df)

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.patient_engagement

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.dim.patient_engagement')

# COMMAND ----------

spark.read.table('mactores.dim.patient_engagement').display()