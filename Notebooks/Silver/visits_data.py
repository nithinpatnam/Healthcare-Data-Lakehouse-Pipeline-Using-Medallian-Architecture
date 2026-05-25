# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.bronze.visits_raw')
display(raw_df)

# COMMAND ----------

raw_df=raw_df.dropDuplicates(subset=['visit_id'])
display(raw_df)

# COMMAND ----------

silver_patient=spark.read.table('mactores.silver.patients_data')
silver_doctor=spark.read.table('mactores.silver.doctors_data')
silver_hospital=spark.read.table('mactores.silver.hospital_data')

# COMMAND ----------

updated_df = raw_df \
    .join(silver_patient, "patient_id", "left_semi") \
    .join(silver_doctor, "doctor_id", "left_semi") \
    .join(silver_hospital, "hospital_id", "left_semi")

display(updated_df)

# COMMAND ----------

final_df=updated_df.withColumn('diagnosis',upper(col('diagnosis')))\
    .withColumn('total_bill',when(col('total_bill')<=0,None).otherwise(col('total_bill')))\
    .withColumn('visit_date',when(col('visit_date')> current_date(),None).otherwise(col('visit_date')))\
    .withColumn('visit_year',year(col('visit_date')))\
    .withColumn('visit_month',month(col('visit_date')))\
    .withColumn("discharge_date",date_add(col("visit_date"),(floor(rand() * 5) + 1).cast("int")))\
    .filter(col('discharge_date')> col('visit_date'))\
    .withColumn("length_of_stay",datediff(col("discharge_date"), col("visit_date")))\
    .fillna("Unkown",subset=['diagnosis'])
display(final_df)




# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.silver.visits_data')

# COMMAND ----------

spark.read.table('mactores.silver.visits_data').display()