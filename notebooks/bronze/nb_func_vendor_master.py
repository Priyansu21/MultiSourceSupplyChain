# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze Ingestion - Vendor Master
# MAGIC
# MAGIC **Purpose:** Ingest the vendor master source CSV into the Bronze Delta table.  
# MAGIC **Source:** data/vendor_master.csv  
# MAGIC **Target:** `supply_chain.ing_bronze.vendor_master`  
# MAGIC **Load type:** Full batch refresh  
# MAGIC
# MAGIC Vendor conformance and dimension logic are handled in the Silver layer.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# COMMAND ----------

import os
source_path = f"file:{os.getcwd()}/../../data/vendor_master.csv"
target_table = "supply_chain.ing_bronze.vendor_master"

# COMMAND ----------

vendor_schema = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("vendor_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("vendor_category", StringType(), True),
    StructField("vendor_status", StringType(), True)
])


# COMMAND ----------

df_vendor = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(vendor_schema)
        .load(source_path)
)


# COMMAND ----------

source_count = df_vendor.count()

if source_count == 0:
    raise ValueError("Vendor master source file contains no records.")

print(f"Source records read: {source_count}")


# COMMAND ----------

(
    df_vendor.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .saveAsTable(target_table)
)


# COMMAND ----------

target_count = spark.table(target_table).count()

if target_count != source_count:
    raise ValueError(
        f"Bronze load validation failed. "
        f"Source count={source_count}, Target count={target_count}"
    )

print(f"Bronze load successful: {target_count} records loaded into {target_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supply_chain.ing_bronze.vendor_master