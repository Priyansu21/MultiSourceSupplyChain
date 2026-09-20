# Databricks notebook source
# MAGIC %md
# MAGIC # **nb_fx_exchange_rates_ref**
# MAGIC
# MAGIC This DDL notebook defines the schema and data types for the `fx_exchange_rates_ref` Silver reference table. The reference dataset provides standardized USD to INR exchange rates used to derive INR-based financial values within the Purchase Order Fulfillment SOT.
# MAGIC
# MAGIC **Created by:** Priyansu Panigrahi  
# MAGIC **Created on:** 17-Sep-2026  
# MAGIC **ETL function:** `nb_func_fx_exchange_rates_ref`  
# MAGIC **Target table:** `fx_exchange_rates_ref`  
# MAGIC **Target schema:** `sot_silver`  
# MAGIC
# MAGIC ### Source Object
# MAGIC
# MAGIC - `supply_chain.ing_bronze.currency_rates`
# MAGIC
# MAGIC ### Logical Key
# MAGIC
# MAGIC - `rate_date`
# MAGIC - `from_currency`
# MAGIC - `to_currency`

# COMMAND ----------

# ============================================================
# FX EXCHANGE RATE REFERENCE
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS supply_chain.sot_silver.fx_exchange_rates_ref
(
    rate_date DATE NOT NULL,
    from_currency STRING NOT NULL,
    to_currency STRING NOT NULL,
    exchange_rate DOUBLE
)
USING DELTA
""")

# COMMAND ----------

spark.sql("""
DESCRIBE TABLE supply_chain.sot_silver.fx_exchange_rates_ref
""").display()