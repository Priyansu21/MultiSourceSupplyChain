# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Ingestion - Manufacturing Batches
# MAGIC
# MAGIC **Purpose:** Ingest the manufacturing source CSV into the Bronze Delta table.
# MAGIC
# MAGIC **Source:** data/manufacturing.csv
# MAGIC
# MAGIC **Target:** supply_chain.ing_bronze.manufacturing_batches
# MAGIC
# MAGIC **Load Strategy:** Full batch refresh
# MAGIC
# MAGIC Business validation and cross-source relationships are handled downstream.


# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)


# COMMAND ----------

notebook_path = dbutils.notebook.getContext().notebookPath().get()

project_root = notebook_path.split("/notebooks/")[0]

source_path = f"file:/Workspace{project_root}/data/manufacturing.csv"

target_table = "supply_chain.ing_bronze.manufacturing_batches"


# COMMAND ----------

manufacturing_schema = StructType([
    StructField("mfg_order_id", StringType(), True),
    StructField("order_ref", StringType(), True),
    StructField("production_date", DateType(), True),
    StructField("materials_used_kg", DoubleType(), True),
    StructField("production_hours", DoubleType(), True),
    StructField("batch_id", StringType(), True)
])


# COMMAND ----------

df_manufacturing = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(manufacturing_schema)
        .load(source_path)
)


# COMMAND ----------

source_count = df_manufacturing.count()

if source_count == 0:
    raise ValueError("Manufacturing source file contains no records.")

print(f"Source records read: {source_count}")


# COMMAND ----------

(
    df_manufacturing.write
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