# Databricks notebook source
# MAGIC %md
# MAGIC # **nb_quarantine_supply_chain**
# MAGIC
# MAGIC This DDL notebook defines the schema and data types for the `quarantine_supply_chain` Silver quarantine table. The table stores Purchase Order Fulfillment records that fail defined data-quality or business validation rules during Silver processing, along with the failure reasons and investigation context.
# MAGIC
# MAGIC **Created by:** Priyansu Panigrahi  
# MAGIC **Created on:** 17-Sep-2026  
# MAGIC **ETL function:** `nb_func_purchase_order_fulfillment_sot`  
# MAGIC **Target table:** `quarantine_supply_chain`  
# MAGIC **Target schema:** `sot_silver`  
# MAGIC
# MAGIC ### Source Objects
# MAGIC
# MAGIC - `supply_chain.ing_bronze.procurement_orders`
# MAGIC - `supply_chain.ing_bronze.vendor_master`
# MAGIC - `supply_chain.ing_bronze.manufacturing_batches`
# MAGIC - `supply_chain.ing_bronze.logistics_shipments`
# MAGIC - `supply_chain.ing_bronze.currency_rates`
# MAGIC
# MAGIC ### Purpose
# MAGIC
# MAGIC Records that fail Silver data-quality validation are retained in this table for investigation instead of being loaded into the trusted `purchase_order_fulfillment_sot` table.

# COMMAND ----------

# ============================================================
# QUARANTINE SUPPLY CHAIN
# Stores records failing Silver data-quality validation.
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS supply_chain.sot_silver.quarantine_supply_chain
(
    -- QUARANTINE METADATA
    source_table          STRING,
    business_key          STRING,
    _quality_flag         STRING,
    _quality_reasons      STRING,
    quarantine_timestamp  TIMESTAMP,

    purchase_order_id     STRING,
    vendor_id             STRING,
    vendor_name STRING,
    vendor_country STRING,
    purchase_order_date DATE,
    order_quantity BIGINT,
    unit_cost_usd DOUBLE,

    manufacturing_order_id STRING,
    production_date DATE,
    materials_consumed_kg DOUBLE,
    production_hours DOUBLE,
    manufacturing_batch_id STRING,
    shipment_id STRING,
    shipment_date DATE,
    actual_delivery_date DATE,
    promised_delivery_date DATE,
    carrier_name STRING,
    logistics_cost_inr DOUBLE,

    usd_to_inr_exchange_rate DOUBLE,
    production_lead_time_days INT,
    shipping_lead_time_days INT,
    total_lead_time_days INT,
    delivery_status STRING,
    dl_create_by STRING
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,validation
spark.sql("""
DESCRIBE TABLE supply_chain.sot_silver.quarantine_supply_chain
""").display()