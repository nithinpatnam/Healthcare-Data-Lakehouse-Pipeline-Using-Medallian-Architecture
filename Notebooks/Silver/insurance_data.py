# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.bronze.insurance_claims_raw')
display(raw_df)

# COMMAND ----------

raw_df=raw_df.dropDuplicates(subset=['claim_id'])
display(raw_df)

# COMMAND ----------

silver_visits=spark.read.table('mactores.silver.visits_data')
silver_patient=spark.read.table('mactores.silver.patients_data')

# COMMAND ----------

checking_df=  raw_df \
    .join(silver_visits, "visit_id", "left_semi") \
    .join(silver_patient, "patient_id", "left_semi")

display(checking_df)

# COMMAND ----------

updated_df=checking_df\
    .join(silver_visits.select('visit_id','total_bill'),'visit_id','left')  \
    .withColumn('claim_status',initcap(col('claim_status')))\
    .fillna('Pending',subset=['claim_status'])\
    .withColumn('claim_amount',when(col('claim_amount')<0,None).otherwise(col('claim_amount')))\
    .withColumn("claim_amount",when(col("claim_amount") > col("total_bill"), col("total_bill")).otherwise(col("claim_amount")))\
    .withColumn('claim_date',when(col('claim_date')> current_date(),None).otherwise(col('claim_date')))\
    .withColumn('claim_ratio', (col('claim_amount')/col('total_bill')))
display(updated_df)


# COMMAND ----------

final_df = updated_df.select(
    "claim_id",
    "visit_id",
    "patient_id",
    "claim_amount",
    "claim_status",
    "claim_date",
    "claim_ratio"
)

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.silver.insurance_data')

# COMMAND ----------

spark.read.table('mactores.silver.insurance_data').display()

# COMMAND ----------

