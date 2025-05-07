# Databricks notebook source
# MAGIC %md
# MAGIC ## Doing transformation for all tables

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
  print(i.name)
  table_name.append(i.name.split('/')[0])
# add directory name to array

# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in table_name:
  path = '/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet'
  #every table which is migrated from on premise sql database is stored in .parquet file in bronze directory (azure datafactory)
  df = spark.read.format('parquet').load(path)
  column = df.columns

  for col in column:
    if "Date" in col or "date" in col:
      df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
      #from data to timestamp

  output_path = '/mnt/silver/SalesLT/' +i +'/'
  df.write.format('delta').mode("overwrite").save(output_path)
  # we save it to delta format, instead of parquet
  #developed by databricks - all the features like parquet + track different version history + better handle schema changing

# COMMAND ----------

display(df)
#always displays the last table - now SalesOrderHeader

# COMMAND ----------


