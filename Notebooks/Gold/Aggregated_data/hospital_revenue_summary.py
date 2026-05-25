# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.gold.final_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.groupBy(col('hospital_name'),col('hospital_city'))\
    .agg(
        sum(col('total_bill')).alias('total_revenue'),
        count(col('visit_id')).alias('total_visits'),
        avg(col('total_bill')).alias('average_bill')
        )
display(updated_df)

# COMMAND ----------

hospital_revenue_data = updated_df.select("hospital_name","hospital_city","total_revenue","total_visits","average_bill")
display(hospital_revenue_data)


# COMMAND ----------

hospital_revenue_data.write.format('delta').saveAsTable('mactores.gold.hospital_revenue_data')

# COMMAND ----------

spark.read.table('mactores.gold.hospital_revenue_data').display()