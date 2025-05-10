# Databricks notebook source
# MAGIC %md
# MAGIC ## Doing transformation for all tables (Changing column names)

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls('mnt/silver/SalesLT/'):
  print(i.name)
  table_name.append(i.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------

for name in table_name:
  path = '/mnt/silver/SalesLT/' + name
  print(path)
  df = spark.read.format('delta').load(path)

  # Get the list of column names
  column_names = df.columns

  for old_col_name in column_names:
      # Convert column name from ColumnName to Column_Name format
      new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
      # enumerate(old_col_name) -> (0, 'C'), (1, 'o'), (2, 'l'), (3, 'u'), (4, 'm'), (5, 'n')
      # "_" + char if char.isupper() and not old_col_name[i - 1].isupper() -> if this sign is uppercase and the previous one wasn't -> "_"+char
      # lstrip("_") -> deletes "_" signs, when it starts the world -> _Column

      # Change the column name using withColumnRenamed and regexp_replace
      df = df.withColumnRenamed(old_col_name, new_col_name)

  output_path = '/mnt/gold/SalesLT/' +name +'/'
  df.write.format('delta').mode("overwrite").save(output_path)

# COMMAND ----------

display(df)

# COMMAND ----------


