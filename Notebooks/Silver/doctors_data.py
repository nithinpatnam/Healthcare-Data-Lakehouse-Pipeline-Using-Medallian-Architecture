# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

raw_df=spark.read.table('mactores.bronze.doctors_raw')
display(raw_df)

# COMMAND ----------

raw_df=raw_df.dropDuplicates(subset=['doctor_id'])
display(raw_df)

# COMMAND ----------

updated_df=raw_df.withColumn('doctor_name',initcap(col('doctor_name')))\
    .withColumn('specialization',when(col('specialization')=="cardio","Cardiology").otherwise(col('specialization')))\
    .withColumn('specialization',initcap(col('specialization')))\
    .withColumn('experience_years',when(col('experience_years')<0,None).otherwise(col('experience_years')))\
    .fillna('General',subset=['specialization'])
display(updated_df)

# COMMAND ----------

final_df=updated_df.withColumn('experience_level',
     when((col('experience_years')>=0) & (col('experience_years')<=5),'Junior')
    .when((col('experience_years')>=6) & (col('experience_years')<=10),'Mid')
    .when(col('experience_years')>10,'Senior')\
    .otherwise("Unkown"))
display(final_df)

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.silver.doctors_data')

# COMMAND ----------

spark.read.table('mactores.silver.doctors_data').display()

# COMMAND ----------

