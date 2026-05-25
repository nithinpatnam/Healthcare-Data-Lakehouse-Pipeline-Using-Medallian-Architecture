# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

raw_df=spark.read.table('mactores.bronze.hospital_raw')
display(raw_df)

# COMMAND ----------

raw_df=raw_df.dropDuplicates(subset=['hospital_id'])
display(raw_df)

# COMMAND ----------

updated_df=raw_df.withColumn("hospital_name",trim(col('hospital_name')))\
    .withColumn('hospital_name',initcap(col('hospital_name')))\
    .withColumn('city',initcap(col('city')))\
    .withColumn('state',initcap(col('state')))\
    .fillna('Unknown',subset=['city','state'])\
    .withColumn('bed_count',when(col('bed_count')<0,None).otherwise(col('bed_count')))
display(updated_df)

# COMMAND ----------

final_df=updated_df.withColumn('hospital_size',when(col('bed_count')<=200,'Small')
                                              .when((col('bed_count')>200) & (col('bed_count')<=500),'Medium')
                                              .when(col('bed_count')>500,'Large')
                                              .otherwise('Unkown'))
display(final_df)

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.silver.hospital_data')

# COMMAND ----------

spark.read.table('mactores.silver.hospital_data').display()