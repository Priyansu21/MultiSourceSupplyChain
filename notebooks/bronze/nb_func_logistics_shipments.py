# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze Ingestion - Logistics Shipments
# MAGIC
# MAGIC **Purpose:** Ingest the logistics shipment source CSV into the Bronze Delta table.  
# MAGIC **Source:** data/logistics.csv  
# MAGIC **Target:** `supply_chain.ing_bronze.logistics_shipments`  
# MAGIC **Load type:** Full batch refresh  
# MAGIC
# MAGIC Temporal and business-quality validation is handled in Silver.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    LongType
)

# COMMAND ----------

import os
source_path = f"file:{os.getcwd()}/../../data/logistics.csv"
target_table = "supply_chain.ing_bronze.logistics_shipments"

# COMMAND ----------

logistics_schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("po_number", StringType(), True),
    StructField("ship_date", DateType(), True),
    StructField("delivery_date", DateType(), True),
    StructField("promised_delivery_date", DateType(), True),
    StructField("carrier", StringType(), True),
    StructField("cost_inr", LongType(), True)
])

# COMMAND ----------

df_logistics = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(logistics_schema)
        .load(source_path)
)


# COMMAND ----------

source_count = df_logistics.count()

if source_count == 0:
    raise ValueError("Logistics source file contains no records.")

print(f"Source records read: {source_count}")


# COMMAND ----------

(
    df_logistics.write
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
# MAGIC select * from  supply_chain.ing_bronze.logistics_shipments