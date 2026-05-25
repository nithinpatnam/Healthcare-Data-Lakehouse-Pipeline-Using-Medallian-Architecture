# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.bronze.patients_events_raw')
display(raw_df)

# COMMAND ----------

raw_df=raw_df.dropDuplicates(subset=['event_id'])
display(raw_df)

# COMMAND ----------

updated_df=raw_df.withColumn('event_type',upper(col('event_type')))\
    .withColumn('department',when(col('department')=='cardio','Cardiology').otherwise(col('department')))\
    .withColumn('department',initcap(col('department')))\
    .fillna('Unknown',subset=['department','priority'])\
    .fillna('UNKNOWN',subset=['event_type'])\
    .withColumn('event_time', to_timestamp(col('event_time')))\
    .withColumn('event_date',to_date(col('event_time')))\
    .withColumn('event_hour',hour(col('event_time')))\
    .withColumn('event_date',when(col('event_date')> current_date(),None).otherwise(col('event_date')))\
    .filter(col('event_date') <= current_date())\
    .withColumn('event_category',when(col('event_type')=='CHECK_IN','Admission')
                .when(col('event_type')=='CHECK_OUT','Discharge')
                .when(col('event_type')=='EMERGENCY','Emergency')
                .otherwise('Other'))\
    .withColumn('is_critical_event',when(col('priority')=='High',1)
                .when(col('priority')=='Medium',0)
                .when(col('priority')=='Low',0)
                .otherwise(0))
display(updated_df)

# COMMAND ----------

silver_patients=spark.read.table('mactores.silver.patients_data')

# COMMAND ----------

final_df=updated_df\
    .join(silver_patients,'patient_id','left_semi')
display(final_df)

# COMMAND ----------

final_df.write.format('delta').saveAsTable("mactores.silver.patient_events_data")

# COMMAND ----------

spark.read.table('mactores.silver.patient_events_data').display()