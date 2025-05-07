# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT/")
# lists all subdirectories/files in this directory

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://silver@m<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://gold@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs)

# COMMAND ----------


