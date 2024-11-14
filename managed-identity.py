# Databricks notebook source
# Configuration for ADLS Gen2 with Managed Identity
configs = {
    "fs.azure.account.auth.type": "ManagedIdentity",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ManagedIdentityTokenProvider"
}

# Replace <CONTAINER_NAME>, <STORAGE_ACCOUNT_NAME>, and <MOUNT_NAME> with your values
dbutils.fs.mount(
    source = "abfss://ecommerce@ecommercesales.dfs.core.windows.net/",
    mount_point = "/mnt/datalake/ecommerce",
    extra_configs = configs
)

print("ADLS Gen2 is successfully mounted using Managed Identity")


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.ecommercesales.dfs.core.windows.net", "ManagedIdentity")
spark.conf.set("fs.azure.account.oauth.provider.type.ecommercesales.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ManagedIdentityTokenProvider")

# Access data directly without mounting

df = spark.read.parquet("abfs://ecommerce@ecommercesales.dfs.core.windows.net/ecommerce/sales-data.csv")
df.show()
