# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

fact_patient_visits=spark.read.table('mactores.fact.fact_patient_visits')
dim_patients=spark.read.table('mactores.dim.dim_patients')
dim_doctor=spark.read.table('mactores.dim.dim_doctors')
dim_hospital=spark.read.table('mactores.dim.dim_hospitals')
fact_insurance_claims=spark.read.table('mactores.fact.fact_insurance_claims')
patient_engagement=spark.read.table('mactores.dim.patient_engagement')

# COMMAND ----------

updated_df=fact_patient_visits\
    .join(dim_patients, "patient_id", "left") \
    .join(dim_doctor, "doctor_id", "left") \
    .join(dim_hospital, "hospital_id", "left") \
    .join(fact_insurance_claims, "visit_id", "left") \
    .join(patient_engagement, "patient_id", "left")
display(updated_df)

# COMMAND ----------

checking_df=updated_df.select(
    "visit_id",  
    "patient_name", 
    "doctor_name",  
    "hospital_name",  
    "hospital_city",  
    "visit_date",  
    "discharge_date", 
    "length_of_stay",  
    "diagnosis",  
    "total_bill",  
    "claim_amount",  
    "claim_status",  
    "insurance_coverage_percent",  
    "engagement_level"
)\
.withColumn('diagnosis',regexp_replace(col('diagnosis'),'Unkown','Unknown'))\
.withColumn('engagement_level',
    when(col('engagement_level').isNull(), "Low")
    .otherwise(col('engagement_level')))\
.withColumn('claim_status',
        when(col('claim_status').isNull(),'No Claim')
        .otherwise(col('claim_status')))\
.withColumn('insurance_coverage_percent',
            when(col('insurance_coverage_percent').isNull(),0)
            .otherwise(col('insurance_coverage_percent')))\
.filter(col('total_bill').isNotNull())\
.withColumn(
    "claim_amount",
    when(
        col("claim_amount") == col("total_bill"),
        col("total_bill") * (0.7 + rand() * 0.3)   
    ).otherwise(col("claim_amount"))
)
display(checking_df)

# COMMAND ----------

final_df=checking_df.select(
    "visit_id",  
    "patient_name", 
    "doctor_name",  
    "hospital_name",  
    "hospital_city",  
    "visit_date",  
    "discharge_date", 
    "length_of_stay",  
    "diagnosis",  
    "total_bill",  
    "claim_amount",  
    "claim_status",  
    "insurance_coverage_percent",  
    "engagement_level"
)
display(final_df)

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.final_data

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.gold.final_data')

# COMMAND ----------

spark.read.table('mactores.gold.final_data').display()