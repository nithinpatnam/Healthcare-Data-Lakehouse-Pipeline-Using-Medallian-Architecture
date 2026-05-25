# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.gold.final_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.withColumn('year',year(col('visit_date')))\
        .withColumn('month',month(col('visit_date')))
display(updated_df)

# COMMAND ----------

final_df=updated_df.groupBy(col('year'),col('month'))\
    .agg(
        count(col('visit_id')).alias('total_visits'),
        sum(col('total_bill')).alias('total_revenue'),
        avg(col('total_bill')).alias('average_bill')
    )
display(final_df)

# COMMAND ----------

monthly_trend=final_df.filter(
    ~((col("year") == 2025) & (col("month") == 3))\
        
).orderBy(col('year'),col('month'))
display(monthly_trend)

# COMMAND ----------

monthly_trend.write.format('delta').saveAsTable('mactores.gold.monthly_trend')


# COMMAND ----------

spark.read.table('mactores.gold.monthly_trend').display()