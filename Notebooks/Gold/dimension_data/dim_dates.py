# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.silver.visits_data')
display(raw_df)

# COMMAND ----------

updated_df=raw_df.withColumn('date',col('visit_date'))\
    .withColumn('year',year(col('visit_date')))\
    .withColumn('month',month(col('visit_date')))\
    .withColumn('day',dayofmonth(col('visit_date')))\
    .withColumn('week',weekofyear(col('visit_date')))
display(updated_df)

# COMMAND ----------

final_df=updated_df.select(
    "date",
    "year",
    "month",
    "day",
    "week"
)
display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists mactores.dim

# COMMAND ----------

# %sql
# drop table if exists mactores.gold.dim_dates

# COMMAND ----------

final_df.write.format('delta').saveAsTable('mactores.dim.dim_dates')

# COMMAND ----------

spark.read.table('mactores.dim.dim_dates').display()