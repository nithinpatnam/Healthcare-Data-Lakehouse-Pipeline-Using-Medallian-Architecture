# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.gold.final_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.groupBy(col('claim_status'))\
    .agg(
        count(col('visit_id')).alias('total_claims'),
        sum(col('claim_amount')).alias('total_claim_amount'),
        avg(col('insurance_coverage_percent')).alias('average_cover')
    )
display(updated_df)

# COMMAND ----------

final_df=updated_df.fillna(0,subset=['total_claim_amount'])
display(final_df)

# COMMAND ----------

insurance_summary=final_df.select('claim_status','total_claims','total_claim_amount','average_cover')
display(insurance_summary)

# COMMAND ----------

insurance_summary.write.format('delta').saveAsTable('mactores.gold.insurance_summary')

# COMMAND ----------

spark.read.table('mactores.gold.insurance_summary').display()