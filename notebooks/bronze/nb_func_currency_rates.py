# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze Ingestion - Currency Rates
# MAGIC
# MAGIC **Purpose:** Ingest the currency reference source CSV into the Bronze Delta table.  
# MAGIC **Source:** data/currency_rates.csv  
# MAGIC **Target:** `supply_chain.ing_bronze.currency_rates`  
# MAGIC **Load type:** Full batch refresh   
# MAGIC
# MAGIC Currency conversion logic is applied downstream in the Silver SOT.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)


# COMMAND ----------

import os
source_path = f"file:{os.getcwd()}/../../data/currency_rates.csv"
target_table = "supply_chain.ing_bronze.currency_rates"

# COMMAND ----------

currency_schema = StructType([
    StructField("rate_date", DateType(), True),
    StructField("from_currency", StringType(), True),
    StructField("to_currency", StringType(), True),
    StructField("exchange_rate", DoubleType(), True)
])


# COMMAND ----------

df_currency = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(currency_schema)
        .load(source_path)
)


# COMMAND ----------

source_count = df_currency.count()

if source_count == 0:
    raise ValueError("Currency rates source file contains no records.")

print(f"Source records read: {source_count}")


# COMMAND ----------

(
    df_currency.write
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
# MAGIC select * from supply_chain.ing_bronze.currency_rates