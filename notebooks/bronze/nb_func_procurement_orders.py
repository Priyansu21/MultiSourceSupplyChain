# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Ingestion - Procurement Orders
# MAGIC
# MAGIC **Purpose:** Ingest the procurement source CSV into the Bronze Delta table.
# MAGIC
# MAGIC **Source:** data/procurement.csv
# MAGIC
# MAGIC **Target:** supply_chain.ing_bronze.procurement_orders
# MAGIC
# MAGIC **Load Strategy:** Full batch refresh
# MAGIC
# MAGIC The Bronze layer preserves the source structure. Business transformations
# MAGIC and data-quality rules are applied in downstream Silver processing.


# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    LongType
)


# COMMAND ----------

# Resolve the Git folder root dynamically.
# This avoids hard-coding the user's Databricks email/path.

notebook_path = dbutils.notebook.getContext().notebookPath().get()

project_root = notebook_path.split("/notebooks/")[0]

source_path = f"file:/Workspace{project_root}/data/procurement.csv"

target_table = "supply_chain.ing_bronze.procurement_orders"


# COMMAND ----------

procurement_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("vendor_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("cost_usd", DoubleType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", LongType(), True),
    StructField("product_type", StringType(), True),
    StructField("payment_terms", StringType(), True)
])


# COMMAND ----------

df_procurement = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(procurement_schema)
        .load(source_path)
)


# COMMAND ----------

# Minimal ingestion validation

source_count = df_procurement.count()

if source_count == 0:
    raise ValueError("Procurement source file contains no records.")

print(f"Source records read: {source_count}")


# COMMAND ----------

# Refresh Bronze table

(
    df_procurement.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .saveAsTable(target_table)
)


# COMMAND ----------

# Target validation

target_count = spark.table(target_table).count()

if target_count != source_count:
    raise ValueError(
        f"Bronze load validation failed. "
        f"Source count={source_count}, Target count={target_count}"
    )

print(f"Bronze load successful: {target_count} records loaded into {target_table}")