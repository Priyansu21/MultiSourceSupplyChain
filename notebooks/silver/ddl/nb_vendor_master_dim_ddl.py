# Databricks notebook source
# MAGIC %md
# MAGIC # **nb_vendor_master_dim**
# MAGIC
# MAGIC This DDL notebook defines the schema and data types for the `vendor_master_dim` Silver dimension table. The dimension provides a conformed vendor representation used to enrich Purchase Order Fulfillment data with standardized vendor attributes.
# MAGIC
# MAGIC **Created by:** Priyansu Panigrahi  
# MAGIC **Created on:** 17-Sep-2026  
# MAGIC **ETL function:** `nb_func_vendor_master_dim`  
# MAGIC **Target table:** `vendor_master_dim`  
# MAGIC **Target schema:** `sot_silver`  
# MAGIC
# MAGIC ### Source Object
# MAGIC
# MAGIC - `supply_chain.ing_bronze.vendor_master`

# COMMAND ----------

# ============================================================
# VENDOR MASTER DIMENSION
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS supply_chain.sot_silver.vendor_master_dim
(
    vendor_id STRING NOT NULL,
    vendor_name STRING,
    country STRING,
    vendor_category STRING,
    vendor_status STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql("""
DESCRIBE TABLE supply_chain.sot_silver.vendor_master_dim
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supply_chain.sot_silver.vendor_master_dim