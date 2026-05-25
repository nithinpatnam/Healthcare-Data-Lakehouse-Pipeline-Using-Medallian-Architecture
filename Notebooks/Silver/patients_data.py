# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('mactores.bronze.patients_raw')
display(raw_df)

# COMMAND ----------

raw_df=raw_df.dropDuplicates(subset=['patient_id'])
display(raw_df      )

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.Arranging the patients_name into the normal form
# MAGIC ##### 2.Converting Gender from 'M' to male and 'F' to female
# MAGIC ##### 3.Calculating the age by creating a column
# MAGIC ##### 4.Null Handling by ading a default columns in the columns which contain null values

# COMMAND ----------

updated_df = raw_df.withColumn("patient_name", initcap(col("patient_name")))\
    .withColumn("gender", regexp_replace(col("gender"), "^F$", "Female"))\
    .withColumn("gender", regexp_replace(col("gender"), "^M$", "Male"))\
    .withColumn('age',year(current_date())-year(col('dob')))\
    .fillna("Unknown", subset=["gender", "city"])

display(updated_df)

# COMMAND ----------

filtered_df=updated_df.filter(
    year(col('dob'))<=year(current_date()) 
)
display(filtered_df)    

# COMMAND ----------

filtered_df.write.format('delta').mode('overwrite').saveAsTable('mactores.silver.patients_data')

# COMMAND ----------

spark.read.table('mactores.silver.patients_data').display()

# COMMAND ----------

